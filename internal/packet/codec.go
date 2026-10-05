package packet

import (
	"encoding/binary"
	"errors"
)

// FixedHeader is the decoded fixed header of one packet.
type FixedHeader struct {
	Type   Type
	Flags  byte
	Length int // remaining length
	// HeaderLen is the total number of bytes (fixed header + length) consumed.
	HeaderLen int
}

// DecodeFixedHeader parses the leading bytes of a packet. The buffer must
// contain at least the complete fixed header.
func DecodeFixedHeader(buf []byte) (FixedHeader, []byte, error) {
	if len(buf) < 2 {
		return FixedHeader{}, nil, malformed("short fixed header")
	}
	pType := Type(buf[0] >> 4)
	flags := buf[0] & 0x0f
	var remLen, multiplier, n int = 0, 1, 1
	for {
		if n >= len(buf) {
			return FixedHeader{}, nil, malformed("truncated remaining length")
		}
		encoded := buf[n]
		n++
		remLen += int(encoded&127) * multiplier
		if encoded&128 == 0 {
			break
		}
		multiplier *= 128
		if multiplier > 128*128*128 || n > 4 {
			return FixedHeader{}, nil, malformed("remaining length exceeds 268435455")
		}
	}
	if remLen < 0 {
		return FixedHeader{}, nil, malformed("negative remaining length")
	}
	h := FixedHeader{Type: pType, Flags: flags, Length: remLen, HeaderLen: n}
	if len(buf) < n+remLen {
		return h, nil, ErrShortBuffer
	}
	return h, buf[n : n+remLen], nil
}

// ErrShortBuffer indicates the full packet body has not arrived yet.
var ErrShortBuffer = malformed("short buffer")

// EncodeFixedHeader builds the leading bytes of a packet.
func EncodeFixedHeader(pType Type, flags byte, remaining int) []byte {
	out := []byte{byte(pType)<<4 | flags}
	for {
		digit := byte(remaining % 128)
		remaining /= 128
		if remaining > 0 {
			digit |= 0x80
		}
		out = append(out, digit)
		if remaining == 0 {
			break
		}
	}
	return out
}

func encodeString(s string) []byte {
	out := make([]byte, 2+len(s))
	binary.BigEndian.PutUint16(out, uint16(len(s)))
	copy(out[2:], s)
	return out
}

func decodeString(body []byte, offset int) (string, int, error) {
		if offset+2 > len(body) {
		return "", 0, malformed("truncated UTF-8 string length")
	}
	n := int(binary.BigEndian.Uint16(body[offset:]))
	offset += 2
	if offset+n > len(body) {
		return "", 0, malformed("truncated UTF-8 string data")
	}
	return string(body[offset : offset+n]), offset + n, nil
}

func decodeBinary(body []byte, offset int) ([]byte, int, error) {
	if offset+2 > len(body) {
		return nil, 0, malformed("truncated binary field length")
	}
	n := int(binary.BigEndian.Uint16(body[offset:]))
	offset += 2
	if offset+n > len(body) {
		return nil, 0, malformed("truncated binary field data")
	}
	return body[offset : offset+n], offset + n, nil
}

// EncodeConnect serializes a CONNECT packet.
func EncodeConnect(c Connect) []byte {
	var vh []byte
	vh = append(vh, encodeString("MQTT")...)
	vh = append(vh, 4) // protocol level: MQTT 3.1.1
	var flags byte
	if c.CleanSession {
		flags |= 0x02
	}
	if c.HasUsername {
		flags |= 0x80
	}
	if c.HasPassword {
		flags |= 0x40
	}
	if c.WillFlag {
		flags |= 0x04
		if c.WillRetain {
			flags |= 0x20
		}
		flags |= (c.WillQoS & 0x03) << 3
	}
	vh = append(vh, flags)
	ka := make([]byte, 2)
	binary.BigEndian.PutUint16(ka, c.KeepAlive)
	vh = append(vh, ka...)

	var payload []byte
	payload = append(payload, encodeString(c.ClientID)...)
	if c.WillFlag {
		payload = append(payload, encodeString(c.WillTopic)...)
		willMsg := make([]byte, 2+len(c.WillMessage))
		binary.BigEndian.PutUint16(willMsg, uint16(len(c.WillMessage)))
		copy(willMsg[2:], c.WillMessage)
		payload = append(payload, willMsg...)
	}
	if c.HasUsername {
		payload = append(payload, encodeString(c.Username)...)
	}
	if c.HasPassword {
		pw := make([]byte, 2+len(c.Password))
		binary.BigEndian.PutUint16(pw, uint16(len(c.Password)))
		copy(pw[2:], c.Password)
		payload = append(payload, pw...)
	}
	body := append(vh, payload...)
	return append(EncodeFixedHeader(CONNECT, 0, len(body)), body...)
}

// DecodeConnect parses the remaining body of a CONNECT packet.
func DecodeConnect(body []byte) (Connect, error) {
	var c Connect
	off := 0
	name, next, err := decodeString(body, off)
	if err != nil {
		return c, err
	}
	off = next
	// MQTT 3.1.1: protocol name must be "MQTT", level 4.
	if name != "MQTT" {
		if name == "MQIsdp" && off < len(body) && body[off] == 3 {
			return c, &UnsupportedProtoError{Level: 3}
		}
		return c, malformed("unknown protocol name %q", name)
	}
	if off >= len(body) {
		return c, malformed("missing protocol level")
	}
	if level := body[off]; level != 4 {
		return c, &UnsupportedProtoError{Level: level}
	}
	off++
	if off >= len(body) {
		return c, malformed("missing connect flags")
	}
	flags := body[off]
	off++
	c.CleanSession = flags&0x02 != 0
	c.WillFlag = flags&0x04 != 0
	c.WillQoS = (flags >> 3) & 0x03
	c.WillRetain = flags&0x20 != 0
	c.HasPassword = flags&0x40 != 0
	c.HasUsername = flags&0x80 != 0
	if flags&0x01 != 0 {
		return c, malformed("reserved connect flag bit must be 0")
	}
	if c.WillQoS == 3 {
		return c, malformed("will QoS must not be 3")
	}
	if !c.WillFlag && (c.WillRetain || c.WillQoS != 0) {
		return c, malformed("will retain/qos set without will flag")
	}
	if off+2 > len(body) {
		return c, malformed("missing keep alive")
	}
	c.KeepAlive = binary.BigEndian.Uint16(body[off:])
	off += 2

	c.ClientID, off, err = decodeString(body, off)
	if err != nil {
		return c, err
	}
	if c.WillFlag {
		c.WillTopic, off, err = decodeString(body, off)
		if err != nil {
			return c, err
		}
		c.WillMessage, off, err = decodeBinary(body, off)
		if err != nil {
			return c, err
		}
	}
	if c.HasUsername {
		c.Username, off, err = decodeString(body, off)
		if err != nil {
			return c, err
		}
	}
	if c.HasPassword {
	c.Password, off, err = decodeBinary(body, off)
	if err != nil {
			return c, err
		}
	}
	if off != len(body) {
		return c, malformed("trailing %d bytes in CONNECT", len(body)-off)
	}
	return c, nil
}

// UnsupportedProtoError means the CONNECT used a protocol name/level the
// broker does not support (e.g. MQTT 3.1). Caller answers CONNACK code 1.
type UnsupportedProtoError struct{ Level byte }

func (e *UnsupportedProtoError) Error() string {
	return "unsupported protocol level"
}

// IsUnsupportedProto reports err.
func IsUnsupportedProto(err error) bool {
	var e *UnsupportedProtoError
	return errors.As(err, &e)
}

// EncodeConnack serializes a CONNACK packet.
func EncodeConnack(ca Connack) []byte {
	flags := byte(0)
	if ca.SessionPresent {
		flags = 1
	}
	return []byte{byte(CONNACK) << 4, 2, flags, ca.ReturnCode}
}

// DecodeConnack parses a CONNACK body.
func DecodeConnack(body []byte) (Connack, error) {
	if len(body) != 2 {
		return Connack{}, malformed("CONNACK body must be 2 bytes")
	}
	if body[0]&0xfe != 0 {
		return Connack{}, malformed("CONNACK reserved bits must be 0")
	}
	return Connack{SessionPresent: body[0] == 1, ReturnCode: body[1]}, nil
}
