// Package mqtt3 contains wire-level primitives for MQTT 3.1.1 (OASIS standard).
package mqtt3

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Control packet types as defined by MQTT 3.1.1.
const (
	TypeRESERVED    = 0
	TypeCONNECT     = 1
	TypeCONNACK     = 2
	TypePUBLISH     = 3
	TypePUBACK      = 4
	TypePUBREC      = 5
	TypePUBREL      = 6
	TypePUBCOMP     = 7
	TypeSUBSCRIBE   = 8
	TypeSUBACK      = 9
	TypeUNSUBSCRIBE = 10
	TypeUNSUBACK    = 11
	TypePINGREQ     = 12
	TypePINGRESP    = 13
	TypeDISCONNECT  = 14
)

// Protocol-level connect return codes.
const (
	ConnAccepted                  = 0x00
	ConnRefusedBadProtocol        = 0x01
	ConnRefusedIdentifierRejected = 0x02
	ConnRefusedServerUnavailable  = 0x03
	ConnRefusedBadUsernameOrPass  = 0x04
	ConnRefusedNotAuthorized      = 0x05
)

// DefaultMaxPacketSize is the default upper bound for a single control packet.
const DefaultMaxPacketSize = 256 * 1024

// Errors used at the packet layer. They are wrapped with context at call sites.
var (
	ErrVarIntTooLarge = errors.New("mqtt3: remaining length exceeds 4 bytes")
	ErrPacketTooLarge = errors.New("mqtt3: packet exceeds configured maximum size")
	ErrShortBuffer    = errors.New("mqtt3: truncated packet")
	ErrBadString      = errors.New("mqtt3: malformed UTF-8 string")
	ErrReservedBitSet = errors.New("mqtt3: reserved flag bit has invalid value")
)

// Packet is a decoded MQTT control packet.
type Packet struct {
	Type     byte
	Flags    byte
	Variable []byte
}

// QoS returns the QoS bits of a PUBLISH fixed header.
func (p *Packet) QoS() byte { return (p.Flags >> 1) & 0x03 }

// Dup returns the DUP flag of a PUBLISH fixed header.
func (p *Packet) Dup() bool { return p.Flags&0x08 != 0 }

// Retain returns the RETAIN flag of a PUBLISH fixed header.
func (p *Packet) Retain() bool { return p.Flags&0x01 != 0 }

// ReadPacket reads one complete control packet.
// maxSize limits the remaining length; 0 means DefaultMaxPacketSize.
func ReadPacket(r io.Reader, maxSize int) (*Packet, error) {
	if maxSize <= 0 {
		maxSize = DefaultMaxPacketSize
	}
	first := make([]byte, 1)
	if _, err := io.ReadFull(r, first); err != nil {
		return nil, err
	}
	fixed := first[0]
	remaining := 0
	multiplier := 1
	for i := 0; i < 4; i++ {
		if _, err := io.ReadFull(r, first); err != nil {
			return nil, err
		}
		b := first[0]
		remaining += int(b&0x7F) * multiplier
		if b&0x80 == 0 {
			break
		}
		if i == 3 {
			return nil, ErrVarIntTooLarge
		}
		multiplier *= 128
	}
	if remaining > maxSize {
		return nil, fmt.Errorf("%w: %d > %d", ErrPacketTooLarge, remaining, maxSize)
	}
	body := make([]byte, remaining)
	if remaining > 0 {
		if _, err := io.ReadFull(r, body); err != nil {
			return nil, err
		}
	}
	return &Packet{Type: fixed >> 4, Flags: fixed & 0x0F, Variable: body}, nil
}

// Encode serializes the packet to a newly allocated byte slice.
func (p *Packet) Encode() []byte {
	out := make([]byte, 0, len(p.Variable)+5)
	out = append(out, p.Type<<4|p.Flags)
	n := len(p.Variable)
	for {
		b := byte(n % 128)
		n /= 128
		if n > 0 {
			b |= 0x80
		}
		out = append(out, b)
		if n == 0 {
			break
		}
	}
	out = append(out, p.Variable...)
	return out
}

// Reader is a cursor over a packet's variable header and payload.
type Reader struct {
	B   []byte
	pos int
}

func NewReader(b []byte) *Reader { return &Reader{B: b} }

// Pos returns the current read offset.
func (r *Reader) Pos() int { return r.pos }

func (r *Reader) Byte() (byte, error) {
	if r.pos+1 > len(r.B) {
		return 0, ErrShortBuffer
	}
	b := r.B[r.pos]
	r.pos++
	return b, nil
}

func (r *Reader) U16() (uint16, error) {
	if r.pos+2 > len(r.B) {
		return 0, ErrShortBuffer
	}
	v := binary.BigEndian.Uint16(r.B[r.pos:])
	r.pos += 2
	return v, nil
}

func (r *Reader) String() (string, error) {
	if r.pos+2 > len(r.B) {
		return "", ErrShortBuffer
	}
	n := int(binary.BigEndian.Uint16(r.B[r.pos:]))
	r.pos += 2
	if r.pos+n > len(r.B) {
		return "", ErrShortBuffer
	}
	s := r.B[r.pos : r.pos+n]
	r.pos += n
	return DecodeString(s)
}

func (r *Reader) Binary() ([]byte, error) {
	if r.pos+2 > len(r.B) {
		return nil, ErrShortBuffer
	}
	n := int(binary.BigEndian.Uint16(r.B[r.pos:]))
	r.pos += 2
	if r.pos+n > len(r.B) {
		return nil, ErrShortBuffer
	}
	s := r.B[r.pos : r.pos+n]
	r.pos += n
	return s, nil
}

func (r *Reader) Remaining() []byte { return r.B[r.pos:] }

// Writer builds a variable header / payload.
type Writer struct{ B []byte }

func NewWriter() *Writer { return &Writer{} }

func (w *Writer) Byte(b byte) *Writer { w.B = append(w.B, b); return w }
func (w *Writer) U16(v uint16) *Writer {
	w.B = append(w.B, byte(v>>8), byte(v))
	return w
}

func (w *Writer) String(s string) (*Writer, error) {
	raw, err := EncodeString(s)
	if err != nil {
		return nil, err
	}
	w.B = append(w.B, raw...)
	return w, nil
}

func (w *Writer) Binary(b []byte) *Writer {
	w.U16(uint16(len(b)))
	w.B = append(w.B, b...)
	return w
}

func (w *Writer) Bytes(b []byte) *Writer { w.B = append(w.B, b...); return w }
