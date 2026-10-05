package broker

import "time"

// session persists MQTT state across connections for a client ID.
// All fields are guarded by the broker mutex.
type session struct {
	clientID string
	clean    bool

	// subscriptions: filter -> granted QoS.
	subs map[string]byte

	// Offline queue of messages for QoS 1/2 delivery (durable sessions only).
	queue []queuedMessage

	// nextPacketID allocates IDs 1..65535 for outbound flows.
	nextPacketID uint16

	// outbound in-flight state, keyed by packet ID. Survives disconnect while
	// the session is durable; cleared on clean sessions.
	outbound map[uint16]*inflightOut

	// inbound QoS2 state: PUBLISH packet IDs awaiting PUBREL.
	inboundRec map[uint16]bool
	// inbound QoS2 packet IDs awaiting PUBCOMP after PUBREL.
	inboundRel map[uint16]bool

	// QoS2 PUBLISH payloads held until the corresponding PUBREL arrives.
	pendingRel map[uint16]pendingRelMsg

	createdAt time.Time
}

type queuedMessage struct {
	topic   string
	payload []byte
	qos     byte // effective QoS, 1 or 2
	retain  bool // true only for retained replay at subscribe time
}

type inflightOut struct {
	topic   string
	payload []byte
	qos     byte
	retain  bool
	phase   byte // 1 = PUBLISH sent; 2 = PUBREC received, PUBREL sent (QoS2)
}

func newSession(id string) *session {
	return &session{
		clientID:     id,
		subs:         make(map[string]byte),
		outbound:     make(map[uint16]*inflightOut),
		inboundRec:   make(map[uint16]bool),
		inboundRel:   make(map[uint16]bool),
		createdAt:    time.Now(),
		nextPacketID: 1,
	}
}

// allocPacketID returns a free packet ID or 0 if all are in flight.
func (s *session) allocPacketID() uint16 {
	start := s.nextPacketID
	for {
		id := s.nextPacketID
		s.nextPacketID++
		if s.nextPacketID == 0 {
			s.nextPacketID = 1
		}
		if _, taken := s.outbound[id]; !taken {
			return id
		}
		if s.nextPacketID == start {
			return 0
		}
	}
}
