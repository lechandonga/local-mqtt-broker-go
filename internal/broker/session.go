package broker

import (
	"net"
	"sync"
	"time"
)

// subState is one subscription: granted QoS plus the original filter.
type subState struct {
	filter string
	qos    byte
}

// pendingMessage is an outbound QoS 1/2 message awaiting acknowledgement.
type pendingMessage struct {
	pub         publishData
	packetID    uint16
	delivery    byte // effective QoS delivered to this subscriber
	dup         bool
	inflight    bool
	receivedRel bool // QoS 2: server side, PUBREL seen
	enqueuedAt  time.Time
}

type publishData struct {
	topic   string
	payload []byte
	qos     byte // publication QoS (before subscription downgrade)
	retain  bool // incoming retain flag (false for will/published delivers)
}

type connState struct {
	conn net.Conn
	done chan struct{} // closed when this connection's read loop exits
}

// session holds MQTT session state. All fields except conn/detach bookkeeping
// are only mutated by the session's current connection goroutine.
type session struct {
	mu sync.Mutex

	broker    *Broker
	clientID  string
	subs      map[string]subState
	queue     []*pendingMessage // not yet sent (offline or rate-limited)
	inflight  map[uint16]*pendingMessage
	nextOutID uint16

	// Inbound QoS 2 state: packet IDs the client published and for which
	// PUBREC (but not PUBCOMP) has been sent.
	inboundQoS2 map[uint16]bool

	cleanStart bool
	attached   bool
	conn       *connState
	createdAt  time.Time
}

func newSession(b *Broker, clientID string, cleanStart bool) *session {
	return &session{
		broker:      b,
		clientID:    clientID,
		subs:        make(map[string]subState),
		inflight:    make(map[uint16]*pendingMessage),
		inboundQoS2: make(map[uint16]bool),
		nextOutID:   1,
		cleanStart:  cleanStart,
		createdAt:   time.Now(),
	}
}

func (s *session) allocPacketID() uint16 {
	for i := 0; i < 65536; i++ {
		id := s.nextOutID
		s.nextOutID++
		if s.nextOutID == 0 {
			s.nextOutID = 1
		}
		if _, used := s.inflight[id]; !used {
			return id
		}
	}
	return 0
}

// queuedCount returns messages not yet fully acknowledged (queue + inflight).
func (s *session) queuedCount() int { return len(s.queue) + len(s.inflight) }

// kick closes the active connection, if any. When sendDisconnect is true a
// DISCONNECT packet is written first (used for client-id takeover per
// §3.1.4-3). Safe to call from other goroutines.
func (s *session) kick(shim net.Conn, sendDisconnect bool) {
	s.mu.Lock()
	cs := s.conn
	if shim != nil && cs != nil && cs.conn != shim {
		// The kicking caller holds an older connection handle; a newer
		// connection already took over.
		s.mu.Unlock()
		return
	}
	s.conn = nil
	s.mu.Unlock()
	if cs != nil {
		if sendDisconnect {
			_, _ = cs.conn.Write(disconnectBytes())
		}
		_ = cs.conn.Close()
	}
}

func disconnectBytes() []byte { return []byte{0xE0, 0x00} }
