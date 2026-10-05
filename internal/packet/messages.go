package packet

import "encoding/binary"

// EncodePublish serializes a PUBLISH packet.
func EncodePublish(p Publish) []byte {
	var vh []byte
	vh = append(vh, encodeString(p.Topic)...)
	if p.QoS > 0 {
		id := make([]byte, 2)
		binary.BigEndian.PutUint16(id, p.PacketID)
		vh = append(vh, id...)
	}
	body := append(vh, p.Payload...)
	var flags byte
	if p.DupFlag {
		flags |= 0x08
	}
	flags |= (p.QoS & 0x03) << 1
	if p.Retain {
		flags |= 0x01
	}
	return append(EncodeFixedHeader(PUBLISH, flags, len(body)), body...)
}

// DecodePublish parses a PUBLISH body using the fixed-header flags.
func DecodePublish(flags byte, body []byte) (Publish, error) {
	var p Publish
	p.DupFlag = flags&0x08 != 0
	p.QoS = (flags >> 1) & 0x03
	p.Retain = flags&0x01 != 0
	off := 0
	var err error
	p.Topic, off, err = decodeString(body, off)
	if err != nil {
		return p, err
	}
	if p.QoS > 0 {
		if off+2 > len(body) {
			return p, malformed("PUBLISH missing packet identifier")
		}
		p.PacketID = binary.BigEndian.Uint16(body[off:])
		off += 2
		if p.PacketID == 0 {
			return p, malformed("PUBLISH packet identifier must not be 0")
		}
	} else if p.DupFlag {
		return p, malformed("DUP must be 0 for QoS 0 PUBLISH")
	}
	if p.QoS == 3 {
		return p, malformed("PUBLISH QoS must not be 3")
	}
	p.Payload = body[off:]
	return p, nil
}

func encodePacketID(t Type, flags byte, id uint16) []byte {
	body := []byte{byte(id >> 8), byte(id)}
	return append(EncodeFixedHeader(t, flags, 2), body...)
}

func decodePacketID(body []byte) (uint16, error) {
	if len(body) != 2 {
		return 0, malformed("packet identifier field must be exactly 2 bytes")
	}
	id := binary.BigEndian.Uint16(body)
	if id == 0 {
		return 0, malformed("packet identifier must not be 0")
	}
	return id, nil
}

// EncodeAck serializes PUBACK/PUBREC/PUBCOMP (fixed flag 0) or PUBREL (2).
func EncodeAck(t Type, id uint16) []byte {
	var flags byte
	if t == PUBREL {
		flags = 0x02
	}
	return encodePacketID(t, flags, id)
}

// DecodeAck parses one of the 4-byte acknowledgement packets, validating
// the fixed-header reserved flags.
func DecodeAck(t Type, flags byte, body []byte) (Ack, error) {
	wantFlags := byte(0)
	if t == PUBREL {
		wantFlags = 0x02
	}
	if flags != wantFlags {
		return Ack{}, malformed("%s reserved flags must be %#x", t, wantFlags)
	}
	id, err := decodePacketID(body)
	if err != nil {
		return Ack{}, err
	}
	return Ack{PacketID: id}, nil
}

// EncodeSubscribe serializes a SUBSCRIBE packet.
func EncodeSubscribe(s Subscribe) []byte {
	var body []byte
	id := make([]byte, 2)
	binary.BigEndian.PutUint16(id, s.PacketID)
	body = append(body, id...)
	for _, f := range s.Filters {
		body = append(body, encodeString(f.Topic)...)
		body = append(body, f.QoS&0x03)
	}
	return append(EncodeFixedHeader(SUBSCRIBE, 0x02, len(body)), body...)
}

// DecodeSubscribe parses a SUBSCRIBE body.
func DecodeSubscribe(flags byte, body []byte) (Subscribe, error) {
	if flags != 0x02 {
		return Subscribe{}, malformed("SUBSCRIBE reserved flags must be 2")
	}
	if len(body) < 3 {
		return Subscribe{}, malformed("SUBSCRIBE too short")
	}
	s := Subscribe{PacketID: binary.BigEndian.Uint16(body[:2])}
	if s.PacketID == 0 {
		return s, malformed("SUBSCRIBE packet identifier must not be 0")
	}
	off := 2
	for off < len(body) {
		topic, next, err := decodeString(body, off)
		if err != nil {
			return s, err
		}
		off = next
		if off >= len(body) {
			return s, malformed("SUBSCRIBE missing requested QoS")
		}
		qos := body[off]
		off++
		if qos > 3 {
			return s, malformed("SUBSCRIBE requested QoS %d invalid", qos)
		}
		s.Filters = append(s.Filters, SubFilter{Topic: topic, QoS: qos})
	}
	if len(s.Filters) == 0 {
		return s, malformed("SUBSCRIBE must contain at least one filter")
	}
	return s, nil
}

// EncodeSuback serializes a SUBACK packet.
func EncodeSuback(sa Suback) []byte {
	body := make([]byte, 0, 2+len(sa.ReturnCodes))
	id := make([]byte, 2)
	binary.BigEndian.PutUint16(id, sa.PacketID)
	body = append(body, id...)
	body = append(body, sa.ReturnCodes...)
	return append(EncodeFixedHeader(SUBACK, 0, len(body)), body...)
}

// DecodeSuback parses a SUBACK body (used by the test client).
func DecodeSuback(body []byte) (Suback, error) {
	if len(body) < 3 {
		return Suback{}, malformed("SUBACK too short")
	}
	sa := Suback{PacketID: binary.BigEndian.Uint16(body[:2])}
	for _, c := range body[2:] {
		if c <= 2 || c == SubackFailure {
			sa.ReturnCodes = append(sa.ReturnCodes, c)
			continue
		}
		return sa, malformed("SUBACK invalid return code %d", c)
	}
	return sa, nil
}

// EncodeUnsubscribe serializes an UNSUBSCRIBE packet.
func EncodeUnsubscribe(u Unsubscribe) []byte {
	var body []byte
	id := make([]byte, 2)
	binary.BigEndian.PutUint16(id, u.PacketID)
	body = append(body, id...)
	for _, f := range u.Filters {
		body = append(body, encodeString(f)...)
	}
	return append(EncodeFixedHeader(UNSUBSCRIBE, 0x02, len(body)), body...)
}

// DecodeUnsubscribe parses an UNSUBSCRIBE body.
func DecodeUnsubscribe(flags byte, body []byte) (Unsubscribe, error) {
	if flags != 0x02 {
		return Unsubscribe{}, malformed("UNSUBSCRIBE reserved flags must be 2")
	}
	if len(body) < 4 {
		return Unsubscribe{}, malformed("UNSUBSCRIBE too short")
	}
	u := Unsubscribe{PacketID: binary.BigEndian.Uint16(body[:2])}
	if u.PacketID == 0 {
	return u, malformed("UNSUBSCRIBE packet identifier must not be 0")
	}
	off := 2
	for off < len(body) {
		f, next, err := decodeString(body, off)
		if err != nil {
			return u, err
		}
		off = next
		u.Filters = append(u.Filters, f)
	}
	if len(u.Filters) == 0 {
	return u, malformed("UNSUBSCRIBE must contain at least one filter")
	}
	return u, nil
}

// EncodeUnsuback serializes an UNSUBACK packet.
func EncodeUnsuback(id uint16) []byte { return encodePacketID(UNSUBACK, 0, id) }

// EncodePingreq serializes PINGREQ.
func EncodePingreq() []byte { return EncodeFixedHeader(PINGREQ, 0, 0) }

// EncodePingresp serializes PINGRESP.
func EncodePingresp() []byte { return EncodeFixedHeader(PINGRESP, 0, 0) }

// EncodeDisconnect serializes DISCONNECT.
func EncodeDisconnect() []byte { return EncodeFixedHeader(DISCONNECT, 0, 0) }
