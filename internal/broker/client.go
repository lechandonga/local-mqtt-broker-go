package broker

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lechandonga/local-mqtt-broker-go/internal/mqtt3"
)

// disconnect reasons used in logs and exposed to tests.
type reason string

const (
	reasonClientDISCONNECT reason = "client_disconnect"
	reasonProtocolError    reason = "protocol_error"
	reasonKeepAliveTimeout reason = "keepalive_timeout"
	reasonSlowConsumer     reason = "slow_consumer"
	reasonTakeover         reason = "session_takeover"
	reasonServerShutdown   reason = "server_shutdown"
	reasonNetworkIO        reason = "network_io"
)

// client models one TCP connection.
type client struct {
	broker *Broker
	conn   net.Conn

	// shutdown synchronization
	closeOnce sync.Once
	closeMu   sync.Mutex
	wrClosed  bool
	doneCh    chan struct{}
	reason    reason

	// writer queue
	writeCh chan []byte
	writeMu sync.Mutex

	// set after successful CONNECT
	mu             sync.Mutex
	clientID       string
	currentSession *session
	cleanSession   bool
	keepAlive      time.Duration
	lastSeen       atomic.Int64 // unix nanos; monotonic-safe via time.Now
	will           *willMessage
}

type willMessage struct {
	topic   string
	payload []byte
	qos     byte
	retain  bool
}

func newClient(b *Broker, conn net.Conn) *client {
	c := &client{
		broker:  b,
		conn:    conn,
		doneCh:  make(chan struct{}),
		writeCh: make(chan []byte, b.cfg.WriteQueueDepth),
	}
	c.lastSeen.Store(time.Now().UnixNano())
	return c
}

func (c *client) serve() {
	defer c.cleanup()

	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		c.writeLoop()
	}()

	// Keep-alive watchdog. 1.5x grace per MQTT 3.1.1 §3.1.3-2. Uses
	// time.Since, which relies on Go's monotonic clock readings so wall-clock
	// adjustments cannot trigger false timeouts.
	watchdogStop := make(chan struct{})
	watchdogExited := make(chan struct{})
	go func() {
		defer close(watchdogExited)
		c.keepaliveLoop(watchdogStop)
	}()

	c.readLoop()

	close(watchdogStop)
	<-watchdogExited
	c.shutdownWrite()
	<-writerDone
}

func (c *client) keepaliveLoop(stop chan struct{}) {
	for {
		c.mu.Lock()
		ka := c.keepAlive
		c.mu.Unlock()
		if ka <= 0 {
			// No keep-alive: only wake on stop, polling lightly.
			select {
			case <-stop:
				return
			case <-time.After(30 * time.Second):
			}
			continue
		}
		deadline := time.Unix(0, c.lastSeen.Load()).Add(ka + time.Duration(float64(ka)*0.5))
		wait := time.Until(deadline)
		if wait < 10*time.Millisecond {
			wait = 10 * time.Millisecond
		}
		t := time.NewTimer(wait)
		select {
		case <-stop:
			t.Stop()
			return
		case <-t.C:
			last := time.Unix(0, c.lastSeen.Load())
			if time.Since(last) > ka+time.Duration(float64(ka)*0.5) {
				c.terminate(reasonKeepAliveTimeout)
				return
			}
		}
	}
}

func (c *client) writeLoop() {
	for data := range c.writeCh {
		c.writeMu.Lock()
		_, err := c.conn.Write(data)
		c.writeMu.Unlock()
		if err != nil {
			c.terminate(reasonNetworkIO)
		}
	}
}

// shutdownWrite closes the writer queue exactly once.
func (c *client) shutdownWrite() {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	if !c.wrClosed {
		c.wrClosed = true
		close(c.writeCh)
	}
}

// writeSync writes a frame directly, serializing with the writer goroutine.
// Used for refusal CONNACKs immediately followed by a socket close, so the
// client is guaranteed to observe the reason code rather than a bare RST.
func (c *client) writeSync(data []byte) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_, _ = c.conn.Write(data)
}

// enqueue hands an encoded packet to the writer. It never blocks the broker:
// when the per-client queue is full the client is marked a slow consumer and
// disconnected. QoS 1/2 messages were already persisted in session state, so
// they are redelivered after reconnect instead of being silently lost.
func (c *client) enqueue(data []byte) bool {
	c.closeMu.Lock()
	if c.wrClosed {
		c.closeMu.Unlock()
		return false
	}
	select {
	case c.writeCh <- data:
		c.closeMu.Unlock()
		return true
	case <-c.doneCh:
		c.closeMu.Unlock()
		return false
	default:
		// Queue saturated: bound the waiting time so a wedged client cannot
		// stall publishers forever. Other connections keep their own queues
		// and goroutines, so blocking here never blocks them.
		c.closeMu.Unlock()
		timer := time.NewTimer(2 * time.Second)
		defer timer.Stop()
		select {
		case c.writeCh <- data:
			return true
		case <-c.doneCh:
			return false
		case <-timer.C:
			c.terminate(reasonSlowConsumer)
			return false
		}
	}
}

func (c *client) terminate(r reason) {
	c.closeOnce.Do(func() {
		c.reason = r
		close(c.doneCh)
		_ = c.conn.Close()
	})
}

func (c *client) readLoop() {
	for {
		pkt, err := mqtt3.ReadPacket(c.conn, c.broker.cfg.MaxPacketSize)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				c.terminate(reasonKeepAliveTimeout)
			} else if errors.Is(err, io.EOF) {
				c.terminate(reasonNetworkIO)
			} else {
				c.terminate(reasonProtocolError)
			}
			return
		}
		c.lastSeen.Store(time.Now().UnixNano())

		var fatal bool
		fatal = c.dispatch(pkt)
		if fatal {
			return
		}
	}
}

func (c *client) dispatch(pkt *mqtt3.Packet) bool {
	// After a takeover the detached socket may still deliver a final frame;
	// only the currently registered connection may touch session state.
	if c.clientID != "" {
		b := c.broker
		b.mu.Lock()
		registered := b.clients[c.clientID] == c
		sess := b.sessions[c.clientID]
		b.mu.Unlock()
		if !registered || sess == nil {
			c.terminate(reasonTakeover)
			return true
		}
		c.currentSession = sess
	}

	// Pre-connect only CONNECT is allowed.
	if c.clientID == "" {
		if pkt.Type != mqtt3.TypeCONNECT {
			c.broker.cfg.Logger.Printf("client=%s rejected packet type=%d before CONNECT", c.conn.RemoteAddr(), pkt.Type)
			c.terminate(reasonProtocolError)
			return true
		}
		return c.handleConnect(pkt)
	}

	switch pkt.Type {
	case mqtt3.TypePUBLISH:
		c.handlePublish(pkt, c.currentSession)
	case mqtt3.TypePUBACK:
		c.handleAck(pkt, mqtt3.TypePUBACK)
	case mqtt3.TypePUBREC:
		c.handleAck(pkt, mqtt3.TypePUBREC)
	case mqtt3.TypePUBREL:
		if pkt.Flags != 0x02 {
			c.protocolViolation("PUBREL reserved flags must be 0010")
			return true
		}
		c.handleAck(pkt, mqtt3.TypePUBREL)
	case mqtt3.TypePUBCOMP:
		c.handleAck(pkt, mqtt3.TypePUBCOMP)
	case mqtt3.TypeSUBSCRIBE:
		if pkt.Flags != 0x02 {
			c.protocolViolation("SUBSCRIBE reserved flags must be 0010")
			return true
		}
		c.handleSubscribe(pkt)
	case mqtt3.TypeUNSUBSCRIBE:
		if pkt.Flags != 0x02 {
			c.protocolViolation("UNSUBSCRIBE reserved flags must be 0010")
			return true
		}
		c.handleUnsubscribe(pkt)
	case mqtt3.TypePINGREQ:
		if len(pkt.Variable) != 0 {
			c.protocolViolation("PINGREQ must have empty body")
			return true
		}
		c.enqueue((&mqtt3.Packet{Type: mqtt3.TypePINGRESP}).Encode())
	case mqtt3.TypeDISCONNECT:
		if len(pkt.Variable) != 0 {
			c.protocolViolation("DISCONNECT must have empty body")
			return true
		}
		c.terminate(reasonClientDISCONNECT)
		return true
	case mqtt3.TypeCONNECT:
		c.protocolViolation("second CONNECT on same connection")
		return true
	default:
		c.protocolViolation("unknown control packet type")
		return true
	}
	return false
}

func (c *client) protocolViolation(detail string) {
	c.broker.cfg.Logger.Printf("client_id=%s protocol_violation=%s", c.clientID, detail)
	c.terminate(reasonProtocolError)
}

func u16id(b []byte) (uint16, bool) {
	if len(b) < 2 {
		return 0, false
	}
	return binary.BigEndian.Uint16(b), true
}
