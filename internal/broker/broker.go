// Package broker implements an in-process MQTT 3.1.1 broker on top of real
// TCP connections. See README.md for the supported protocol subset and the
// delivery guarantees of each QoS level.
package broker

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/lechandonga/local-mqtt-broker-go/internal/packet"
)

// Broker accepts MQTT client connections and routes messages.
type Broker struct {
	cfg    Config
	log    Logger
	listen string

	mu          sync.Mutex
	sessions    map[string]*session
	retained    map[string]retainedMessage
	listener    net.Listener
	closed      bool
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	clientSeq   int
	acceptedCnt int
}

type retainedMessage struct {
	payload []byte
	qos     byte
	retain  bool
	topic   string
}

// Option customizes a Broker.
type Option func(*Broker)

// WithLogger overrides the default stdout logger.
func WithLogger(l Logger) Option { return func(b *Broker) { b.log = l } }

// WithConfig overrides resource limits.
func WithConfig(c Config) Option { return func(b *Broker) { b.cfg = c } }

// New creates a broker bound to listenAddr (e.g. "127.0.0.1:0").
func New(listenAddr string, opts ...Option) *Broker {
	ctx, cancel := context.WithCancel(context.Background())
	b := &Broker{
		cfg:      Config{},
		log:      defaultLogger(),
		listen:   listenAddr,
		sessions: make(map[string]*session),
		retained: make(map[string]retainedMessage),
		ctx:      ctx,
		cancel:   cancel,
	}
	for _, o := range opts {
		o(b)
	}
	b.cfg = b.cfg.withDefaults()
	return b
}

// Start binds the TCP listener. It must be called before Serve or Addr.
func (b *Broker) Start() error {
	ln, err := net.Listen("tcp", b.listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	b.listener = ln
	b.log.Printf("[lifecycle] broker listening on %s (limits: packet=%d id=%d inflight=%d queue=%d)",
		ln.Addr(), b.cfg.MaxPacketSize, b.cfg.MaxClientIDLen, b.cfg.MaxInflight, b.cfg.MaxQueuedMessages)
	return nil
}

// Addr returns the bound address, useful with port 0 in tests.
func (b *Broker) Addr() net.Addr { return b.listener.Addr() }

// Serve accepts connections until Close.
func (b *Broker) Serve() {
	for {
		conn, err := b.listener.Accept()
		if err != nil {
			b.mu.Lock()
			closed := b.closed
			b.mu.Unlock()
			if closed {
				return
			}
			b.log.Printf("[lifecycle] accept error: %v", err)
			return
		}
		b.mu.Lock()
		b.acceptedCnt++
		b.mu.Unlock()
		b.wg.Add(1)
		go func(c net.Conn) {
			defer b.wg.Done()
			newConn(b, c).serve()
		}(conn)
	}
}

// Close stops listening and terminates all connections. Persistent session
// state (subscriptions and queued QoS 1/2 messages) is retained in memory
// until the broker itself is discarded.
func (b *Broker) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	ln := b.listener
	sessions := make([]*session, 0, len(b.sessions))
	for _, s := range b.sessions {
		sessions = append(sessions, s)
	}
	b.mu.Unlock()
	b.cancel()
	if ln != nil {
		_ = ln.Close()
	}
	for _, s := range sessions {
		s.kick(nil, true)
	}
	b.wg.Wait()
	b.log.Printf("[lifecycle] broker stopped")
	return nil
}

// Stats describes broker state for tests and diagnostics.
type Stats struct {
	Sessions          int
	PersistentOnline  int
	PersistentOffline int
	Retained          int
	AcceptedConns     int
}

// Stats returns a point-in-time snapshot.
func (b *Broker) Stats() Stats {
	b.mu.Lock()
	defer b.mu.Unlock()
	st := Stats{Retained: len(b.retained), Sessions: len(b.sessions), AcceptedConns: b.acceptedCnt}
	for _, s := range b.sessions {
		s.mu.Lock()
		if s.cleanStart {
			s.mu.Unlock()
			continue
		}
		if s.conn != nil {
			st.PersistentOnline++
		} else {
			st.PersistentOffline++
		}
		s.mu.Unlock()
	}
	return st
}

var _ = packet.CONNECT // keep import in this file even if unused later
