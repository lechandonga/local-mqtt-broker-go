// Package packet implements MQTT 3.1.1 (OASIS standard) control packet
// encoding and decoding.
//
// Only the wire layer lives here: every Decode function returns a
// MalformedError for structurally invalid bytes so callers can distinguish a
// protocol violation from a normal disconnect.
package packet

import (
	"errors"
	"fmt"
)

// Type is the MQTT control packet type (first nibble of byte 1).
type Type byte

const (
	CONNECT     Type = 1
	CONNACK     Type = 2
	PUBLISH     Type = 3
	PUBACK      Type = 4
	PUBREC      Type = 5
	PUBREL      Type = 6
	PUBCOMP     Type = 7
	SUBSCRIBE   Type = 8
	SUBACK      Type = 9
	UNSUBSCRIBE Type = 10
	UNSUBACK    Type = 11
	PINGREQ     Type = 12
	PINGRESP    Type = 13
	DISCONNECT  Type = 14
)

func (t Type) String() string {
	switch t {
	case CONNECT:
		return "CONNECT"
	case CONNACK:
		return "CONNACK"
	case PUBLISH:
		return "PUBLISH"
	case PUBACK:
		return "PUBACK"
	case PUBREC:
		return "PUBREC"
	case PUBREL:
		return "PUBREL"
	case PUBCOMP:
		return "PUBCOMP"
	case SUBSCRIBE:
		return "SUBSCRIBE"
	case SUBACK:
		return "SUBACK"
	case UNSUBSCRIBE:
		return "UNSUBSCRIBE"
	case UNSUBACK:
		return "UNSUBACK"
	case PINGREQ:
		return "PINGREQ"
	case PINGRESP:
		return "PINGRESP"
	case DISCONNECT:
		return "DISCONNECT"
	default:
		return "UNKNOWN"
	}
}

// MalformedError marks a protocol error that must close the network
// connection per MQTT 3.1.1 section 3.1 / 4.8.
type MalformedError struct{ msg string }

func (e *MalformedError) Error() string { return "malformed packet: " + e.msg }

func malformed(format string, args ...any) error {
	return &MalformedError{msg: fmt.Sprintf(format, args...)}
}

// IsMalformed reports whether err is a MalformedError.
func IsMalformed(err error) bool {
	var me *MalformedError
	return errors.As(err, &me)
}

// Connect is a CONNECT packet.
type Connect struct {
	ClientID     string
	CleanSession bool
	KeepAlive    uint16 // seconds
	WillFlag     bool
	WillQoS      byte
	WillRetain   bool
	WillTopic    string
	WillMessage  []byte
	Username     string
	Password     []byte
	HasUsername  bool
	HasPassword  bool
}

// Connack is a CONNACK packet.
type Connack struct {
	SessionPresent bool
	ReturnCode     byte
}

// Connect return codes (MQTT 3.1.1 section 3.2.2.3).
const (
	CodeAccepted             byte = 0
	CodeBadProto             byte = 1
	CodeIDRejected           byte = 2
	CodeServerUnavailable    byte = 3
	CodeBadUsernameOrPasswd  byte = 4
	CodeNotAuthorized        byte = 5
)

// Publish is a PUBLISH packet.
type Publish struct {
	DupFlag  bool
	QoS      byte
	Retain   bool
	Topic    string
	PacketID uint16 // 0 for QoS 0
	Payload  []byte
}

// Subscribe is a SUBSCRIBE packet.
type Subscribe struct {
	PacketID uint16
	Filters  []SubFilter
}

// SubFilter is one topic filter / QoS pair in a SUBSCRIBE.
type SubFilter struct {
	Topic string
	QoS   byte
}

// Suback is a SUBACK packet.
type Suback struct {
	PacketID uint16
	// ReturnCodes uses 0/1/2 for granted QoS, or 0x80 for failure.
	ReturnCodes []byte
}

// SubackFailure rejects a filter (illegal topic or QoS > 2).
const SubackFailure byte = 0x80

// Unsubscribe is an UNSUBSCRIBE packet.
type Unsubscribe struct {
	PacketID uint16
	Filters  []string
}

// Ack carries the packet identifier of any 4-byte ack packet
// (PUBACK/PUBREC/PUBREL/PUBCOMP/UNSUBACK).
type Ack struct {
	PacketID uint16
}
