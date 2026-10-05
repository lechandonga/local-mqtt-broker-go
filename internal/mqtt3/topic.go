package mqtt3

import "strings"

// ValidatePublishTopic checks a topic name used in PUBLISH packets.
// Wildcards are illegal and the topic must be non-empty and UTF-8 valid.
func ValidatePublishTopic(topic string) error {
	if topic == "" {
		return ErrBadTopic
	}
	if strings.ContainsRune(topic, '#') || strings.ContainsRune(topic, '+') {
		return ErrBadTopic
	}
	return validateTopicUTF8(topic)
}

// ValidateFilter checks a topic filter used in SUBSCRIBE packets.
func ValidateFilter(filter string) error {
	if filter == "" {
		return ErrBadFilter
	}
	if err := validateTopicUTF8(filter); err != nil {
		return ErrBadFilter
	}
	levels := strings.Split(filter, "/")
	for i, level := range levels {
		switch {
		case level == "#":
			// '#' must occupy a whole level and be the last level.
			if i != len(levels)-1 {
				return ErrBadFilter
			}
		case strings.ContainsRune(level, '#'):
			return ErrBadFilter
		case strings.ContainsRune(level, '+') && level != "+":
			// '+' must occupy the whole level.
			return ErrBadFilter
		}
	}
	return nil
}

func validateTopicUTF8(topic string) error {
	if !isValidUTF8(topic) {
		return ErrBadTopic
	}
	// NUL is never legal inside an MQTT string.
	if strings.ContainsRune(topic, 0) {
		return ErrBadTopic
	}
	return nil
}

// Match reports whether the topic name matches the filter per MQTT 3.1.1 §4.7.
func Match(filter, topic string) bool {
	if topic == "" {
		return false // topic names are never empty in MQTT
	}
	f := strings.Split(filter, "/")
	t := strings.Split(topic, "/")
	for i := 0; i < len(f); i++ {
		if f[i] == "#" {
			// "sport/#": matches "sport" itself and everything beneath.
			return true
		}
		if i >= len(t) {
			return false
		}
		if f[i] != "+" && f[i] != t[i] {
			return false
		}
	}
	return len(f) == len(t)
}
