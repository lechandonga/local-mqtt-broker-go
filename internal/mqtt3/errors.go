package mqtt3

import "unicode/utf8"

// Protocol-level topic errors. The broker maps these onto close behaviors or
// SUBACK failure codes so callers can distinguish the causes.

type topicError struct{ msg string }

func (e *topicError) Error() string { return "mqtt3: " + e.msg }

var (
	ErrBadTopic  = &topicError{msg: "invalid publish topic name"}
	ErrBadFilter = &topicError{msg: "invalid topic filter"}
)

func isValidUTF8(s string) bool { return utf8.ValidString(s) }
