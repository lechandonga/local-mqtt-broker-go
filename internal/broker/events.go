package broker

import "github.com/lechandonga/local-mqtt-broker-go/internal/packet"

// eventType enumerates messages from connection goroutines to the single
// broker event-loop goroutine. All mutable session/routing state lives behind
// that loop, which is why a wedged slow consumer can never block delivery to
// anyone else.
type eventType int

const (
	evConnect eventType = iota
	evSubscribe
	evUnsubscribe
	evPublish
	evAck
	evPing
	evDisconnect
	evGone // connection ended (clean or abnormal)
	evWriteReady
)

type event struct {
	kind     eventType
	gen      uint64 // connection generation, guards stale callbacks
	clientID string

	connect *packet.Connect
	sub     *packet.Subscribe
	unsub   *packet.Unsubscribe
	pub     *packet.Publish
	ack     ackEvent

	cleanDisconnect bool
	abnormalReason  string
}

type ackEvent struct {
	pType packet.Type
	id    uint16
}
