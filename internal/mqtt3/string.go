package mqtt3

import (
	"encoding/binary"
	"unicode/utf8"
)

// EncodeString encodes s as an MQTT UTF-8 encoded string (2-byte length prefix).
func EncodeString(s string) ([]byte, error) {
	if len(s) > 65535 {
		return nil, ErrBadString
	}
	if !utf8.ValidString(s) {
		return nil, ErrBadString
	}
	// MQTT 3.1.1 forbids U+0000 and the UTF-16 surrogates; other control
	// characters are allowed (they were only restricted in MQTT 5).
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == 0 || (r >= 0xD800 && r <= 0xDFFF) {
			return nil, ErrBadString
		}
		i += size
	}
	out := make([]byte, 2+len(s))
	binary.BigEndian.PutUint16(out, uint16(len(s)))
	copy(out[2:], s)
	return out, nil
}

// DecodeString validates an MQTT UTF-8 encoded string.
func DecodeString(b []byte) (string, error) {
	s := string(b)
	if !utf8.ValidString(s) {
		return "", ErrBadString
	}
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == 0 || (r >= 0xD800 && r <= 0xDFFF) {
			return "", ErrBadString
		}
		i += size
	}
	return s, nil
}
