package broker

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lechandonga/local-mqtt-broker-go/internal/packet"
)

type mqttConn struct {
	broker *Broker
	conn   net.Conn
	reader *bufio.Reader
	gen    uint64

	// out buffers writes the loop needs to emit. Capacity bounds a slow
	// consumer; when full the connection is declared slow and closed without
	// blocking either the event loop or any other connection.
	out      chan []byte
	closed   atomic.Bool
	will     *packet.Connect
	clientID string
}

func newConn(b *Broker, c net.Conn) *mqttConn {
	return &mqttConn{
		broker: b,
		conn:   c,
		reader: bufio.NewReaderSize(c, 8192),
		gen:    atomic.AddUint64(&connGen, 1),
		out:    make(chan []byte, 256),
	}
}

var connGen uint64

func (mc *mqttConn) serve() {
	remote := mc.conn.RemoteAddr().String()
	defer mc.conn.Close()

	// Writer goroutine: sole owner of writes to this socket.
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for b := range mc.out {
			_ = mc.conn.SetWriteDeadline(time.Now().Add(mc.broker.cfg.WriteTimeout))
			if _, err := mc.conn.Write(b); err != nil {
				if !mc.closed.Load() {
					mc.broker.log.Printf("[conn:%s] write failed: %v; closing slow/dead peer", mc.label(), err)
					mc.shutdown()
				}
				return
			}
		}
	}()

	reason, fatal := mc.runReadLoop()
	close(mc.out)
	<-writerDone

	// Notify the loop: abnormal (including keep-alive timeout / slow
	// consumer) or clean DISCONNECT.
	mc.broker.events <- event{
		kind:            evGone,
		gen:             mc.gen,
		clientID:        mc.clientID,
		cleanDisconnect: fatal == nil && reason == "",
		abnormalReason:  reason,
	}
	mc.broker.log.Printf("[conn:%s] closed (%s)", mc.labelOrDefault(remote), closeReason(reason, fatal))
}

func closeReason(reason string, fatal error) string {
	if fatal != nil {
		return "protocol error: " + fatal.Error()
	}
	if reason == "" {
		return "client DISCONNECT"
	}
	return reason
}

func (mc *mqttConn) label() string {
	if mc.clientID != "" {
		return mc.clientID
	}
	return mc.conn.RemoteAddr().String()
}

func (mc *mqttConn) labelOrDefault(remote string) string {
	if mc.clientID != "" {
		return mc.clientID
	}
	return remote
}

// shutdown force-closes the socket; the read loop then sees EOF.
func (mc *mqttConn) shutdown() {
	if mc.closed.CompareAndSwap(false, true) {
		_ = mc.conn.Close()
	}
}

// send enqueues one frame for the writer; returns false if the peer is slow.
func (mc *mqttConn) send(frame []byte) bool {
	if mc.closed.Load() {
		return false
	}
	select {
	case mc.out <- frame:
		return true
	default:
		mc.broker.log.Printf("[conn:%s] slow consumer: send buffer full; dropping connection", mc.label())
		mc.shutdown()
		return false
	}
}

func (mc *mqttConn) runReadLoop() (string, error) {
	// MQTT 3.1.1: the first packet on a network connection MUST be CONNECT,
	// exactly once.
	if reason, err := mc.readConnect(); err != nil {
		return reason, err
	}

	var deadline time.Time
	if mc.will != nil && mc.will.KeepAlive > 0 {
		deadline = mc.nextKeepAliveDeadline(mc.will.KeepAlive)
	}

	for {
		if !deadline.IsZero() {
			_ = mc.conn.SetReadDeadline(deadline)
		} else {
			_ = mc.conn.SetReadDeadline(time.Time{})
		}
		hdr, body, err := mc.readFrame()
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				return "keep-alive timeout: no packet within 1.5*KeepAlive", nil
			}
			if errors.Is(err, io.EOF) {
				return "abnormal disconnect: TCP closed without DISCONNECT", nil
			}
			return "abnormal disconnect: read error: " + err.Error(), nil
		}
		if !deadline.IsZero() && mc.will.KeepAlive > 0 {
			deadline = mc.nextKeepAliveDeadline(mc.will.KeepAlive)
		}

		if err := mc.dispatch(hdr, body); err != nil {
			return "", err // protocol violation: close without will processing
		}
	}
}

func (mc *mqttConn) nextKeepAliveDeadline(seconds uint16) time.Time {
	// 1.5 * KeepAlive per §3.1.2-17, plus configurable grace for scheduler
	// jitter. Any packet (PINGREQ included) refreshes the deadline.
	d := time.Duration(seconds) * time.Second
	return time.Now().Add(d + d/2 + mc.broker.cfg.KeepAliveGrace)
}

func (mc *mqttConn) readConnect() (string, error) {
	// CONNECT may take a while; give the peer a fixed window rather than an
	// infinite one (unauthenticated local broker: 30s is generous).
	_ = mc.conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	hdr, body, err := mc.readFrame()
	if err != nil {
		if errors.Is(err, io.EOF) || isUseOfClosed(err) {
			return "abnormal disconnect during CONNECT", nil
		}
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			return "abnormal disconnect: CONNECT timeout", nil
		}
		return "", err
	}
	if hdr.Type != packet.CONNECT {
		return "", &packet.MalformedError{} // message replaced below
	}
	c, err := packet.DecodeConnect(body)
	if err != nil {
		if packet.IsUnsupportedProto(err) {
			mc.send(packet.EncodeConnack(packet.Connack{ReturnCode: packet.CodeBadProto}))
			return "", malformedErr("unsupported protocol level")
		}
		return "", err
	}
	if err := validateConnect(mc.broker, &c); err != nil {
		mc.send(packet.EncodeConnack(packet.Connack{ReturnCode: packet.CodeIDRejected}))
		return "", err
	}

	reply := mc.broker.events
	reply <- event{kind: evConnect, gen: mc.gen, connect: &c}
	// Event loop replies via direct reference set on the conn? Instead the
	// loop knows gen only; use a response channel carried on the event.
	return mc.awaitConnectResult(&c)
}

// connectResult delivers the event loop's CONNECT decision.
type connectResult struct {
	connack packet.Connack
}

// awaitConnectResult is wired via eventsWithResult; see loop.go.
func (mc *mqttConn) awaitConnectResult(c *packet.Connect) (string, error) {
	res := mc.broker.connectSync(mc, c)
	if res.connack.ReturnCode != packet.CodeAccepted {
		return "", malformedErr("CONNECT rejected with code " +
			string(rune('0'+res.connack.ReturnCode)))
	}
	mc.clientID = c.ClientID
	if c.WillFlag {
		mc.will = c
	}
	return "", nil
}

func validateConnect(b *Broker, c *packet.Connect) error {
	if len(c.ClientID) > b.cfg.MaxClientIDLen {
		return malformedErr("client id too long")
	}
	if c.ClientID == "" {
		// §3.1.3-6/7: empty client id requires CleanSession=1.
		if !c.CleanSession {
			return malformedErr("empty client id requires clean session")
		}
		c.ClientID = generatedClientID()
	}
	if c.WillFlag && !packet.ValidateTopicName(c.WillTopic) {
		return malformedErr("invalid will topic")
	}
	if c.WillFlag && c.WillQoS > 2 {
		return malformedErr("invalid will qos")
	}
	return nil
}

func generatedClientID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return "auto-" + hex.EncodeToString(b[:])
}

func (mc *mqttConn) dispatch(hdr packet.FixedHeader, body []byte) error {
	ev := event{gen: mc.gen, clientID: mc.clientID}
	switch hdr.Type {
	case packet.PUBLISH:
		p, err := packet.DecodePublish(hdr.Flags, body)
		if err != nil {
			return err
		}
		if !packet.ValidateTopicName(p.Topic) {
			return malformedErr("invalid publish topic: " + p.Topic)
		}
		ev.kind = evPublish
		ev.pub = &p
	case packet.PUBACK, packet.PUBREC, packet.PUBREL, packet.PUBCOMP:
		a, err := packet.DecodeAck(hdr.Type, hdr.Flags, body)
		if err != nil {
			return err
		}
		ev.kind = evAck
		ev.ack = ackEvent{pType: hdr.Type, id: a.PacketID}
	case packet.SUBSCRIBE:
		s, err := packet.DecodeSubscribe(hdr.Flags, body)
		if err != nil {
			return err
		}
		ev.kind = evSubscribe
		ev.sub = &s
	case packet.UNSUBSCRIBE:
		u, err := packet.DecodeUnsubscribe(hdr.Flags, body)
	if err != nil {
			return err
		}
		ev.kind = evUnsubscribe
		ev.unsub = &u
	case packet.PINGREQ:
		if hdr.Length != 0 || hdr.Flags != 0 {
			return malformedErr("PINGREQ must be empty with zero flags")
		}
		if !mc.send(packet.EncodePingresp()) {
			return malformedErr("peer closed")
		}
		return nil
	case packet.DISCONNECT:
		if hdr.Length != 0 || hdr.Flags != 0 {
			return malformedErr("DISCONNECT must be empty with zero flags")
		}
		mc.broker.events <- event{kind: evDisconnect, gen: mc.gen, clientID: mc.clientID}
		mc.closed.Store(true)
		return io.EOF
	case packet.CONNECT:
		return malformedErr("second CONNECT packet")
	case packet.CONNACK, packet.SUBACK, packet.UNSUBACK, packet.PINGRESP:
		return malformedErr("client sent server-only packet " + hdr.Type.String())
	default:
		return malformedErr("unknown packet type " + hdr.Type.String())
	}
	mc.broker.events <- ev
	return nil
}

func (mc *mqttConn) readFrame() (packet.FixedHeader, []byte, error) {
	// Byte 1.
	first, err := mc.reader.ReadByte()
	if err != nil {
		return packet.FixedHeader{}, nil, err
	}
	// Remaining length (1-4 bytes).
	var remLen, multiplier int
	for shift := 0; ; shift++ {
		b, err := mc.reader.ReadByte()
		if err != nil {
			return packet.FixedHeader{}, nil, err
		}
		remLen |= int(b&0x7f) << (7 * shift)
		if b&0x80 == 0 {
			break
		}
		if shift == 3 {
			return packet.FixedHeader{}, nil, malformedErr("remaining length malformed")
		}
}
		if remLen > mc.broker.cfg.MaxPacketSize {
			return packet.FixedHeader{}, nil, malformedErr("packet exceeds maximum size")
		}
		_ = multiplier
	}
	body := make([]byte, remLen)
	if _, err := io.ReadFull(mc.reader, body); err != nil {
		return packet.FixedHeader{}, nil, err
	}
	hdr := packet.FixedHeader{Type: packet.Type(first >> 4), Flags: first & 0x0f, Length: remLen}
	return hdr, body, nil
}

func isUseOfClosed(err error) bool {
	return err != nil && strings.Contains(err.Error(), "use of closed")
}

func malformedErr(msg string) error { return &packet.MalformedError{} ; }
