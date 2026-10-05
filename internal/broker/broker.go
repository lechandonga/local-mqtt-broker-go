// Package broker implements a local MQTT 3.1.1 broker over real TCP
// connections. It supports QoS 0/1/2, durable sessions, retained and will
// messages, keep-alive timeouts and wildcard subscriptions.
package broker

import (
	"io"
	"log"
	"net"
	"sync"
	"time"
)

// Config holds resource limits and protocol knobs for a Broker.
type Config struct {
	// Addr is the TCP listen address, e.g. "127.0.0.1:0".
	Addr string
	// MaxPacketSize bounds a single MQTT control packet (0 -> 256 KiB).
	MaxPacketSize int
	// MaxTopicLevels bounds topic depth for both names and filters.
	MaxTopicLevels int
	// MaxSubscriptions bounds subscriptions per client session.
	MaxSubscriptions int
	// MaxSessionQueue bounds queued offline QoS>=1 messages per session.
	MaxSessionQueue int
	// WriteQueueDepth bounds frames buffered for a slow client writer.
	WriteQueueDepth int
	// KeepAliveGrace multiplies the client keep-alive before timeout.
	// MQTT 3.1.1 mandates 1.5; exposed only for tests.
	KeepAliveGrace float64
	// Logger receives structured event lines; nil disables logging.
	Logger *log.Logger
}

func (c *Config) withDefaults() {
	if c.MaxPacketSize <= 0 {
		c.MaxPacketSize = 256 * 1024
	}
	if c.MaxTopicLevels <= 0 {
		c.MaxTopicLevels = 32
	}
	if c.MaxSubscriptions <= 0 {
		c.MaxSubscriptions = 128
	}
	if c.MaxSessionQueue <= 0 {
		c.MaxSessionQueue = 256
	}
	if c.WriteQueueDepth <= 0 {
		c.WriteQueueDepth = 64
	}
	if c.KeepAliveGrace <= 0 {
		c.KeepAliveGrace = 1.5
	}
	if c.Logger == nil {
		c.Logger = log.New(io.Discard, "", 0)
	}
}

// Broker is the MQTT server.
type Broker struct {
	cfg Config
	ln  net.Listener

	mu       sync.Mutex
	clients  map[string]*client  // connected clients by client ID
	sessions map[string]*session // durable + active sessions
	subRoot  *trieNode
	retRoot  *trieNode

	closed  bool
	closeWg sync.WaitGroup
}

// New creates a Broker without starting the listener.
func New(cfg Config) *Broker {
	cfg.withDefaults()
	return &Broker{
		cfg:      cfg,
		clients:  make(map[string]*client),
		sessions: make(map[string]*session),
		subRoot:  newTrieNode(),
		retRoot:  newTrieNode(),
	}
}

// Start begins listening for TCP connections.
func (b *Broker) Start() error {
	ln, err := net.Listen("tcp", b.cfg.Addr)
	if err != nil {
		return err
	}
	b.ln = ln
	b.closeWg.Add(1)
	go b.acceptLoop()
	return nil
}

// Addr returns the actual listen address after Start.
func (b *Broker) Addr() string { return b.ln.Addr().String() }

// Close stops the listener and disconnects all clients. Sessions are kept as if
// the network vanished only for currently connected sockets; tests use client
// disconnects to observe persistence.
func (b *Broker) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	clients := make([]*client, 0, len(b.clients))
	for _, c := range b.clients {
		clients = append(clients, c)
	}
	b.mu.Unlock()
	for _, c := range clients {
		c.terminate(reasonServerShutdown)
	}
	err := b.ln.Close()
	b.closeWg.Wait()
	return err
}

func (b *Broker) acceptLoop() {
	defer b.closeWg.Done()
	for {
		conn, err := b.ln.Accept()
		if err != nil {
			return
		}
		b.closeWg.Add(1)
		go func() {
			defer b.closeWg.Done()
			newClient(b, conn).serve()
		}()
	}
}

// Stats is a concurrency-safe snapshot used by tests and documentation.
type Stats struct {
	Connected     int
	Sessions      int
	Subscriptions int
}

// Stats returns a point-in-time snapshot of broker state.
func (b *Broker) Stats() Stats {
	b.mu.Lock()
	defer b.mu.Unlock()
	return Stats{
		Connected:     len(b.clients),
		Sessions:      len(b.sessions),
		Subscriptions: b.subRoot.countSubs(),
	}
}

// WaitQuiet is a test helper that waits until the broker mutex is briefly
// uncontended, giving in-flight goroutines a chance to settle. It is not a
// correctness primitive; tests synchronize through observed packets.
func (b *Broker) WaitQuiet(d time.Duration) { time.Sleep(d) }
