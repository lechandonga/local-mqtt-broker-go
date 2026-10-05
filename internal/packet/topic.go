package packet

// ValidateTopicName checks a PUBLISH topic name: non-empty, no wildcards,
// no NUL, length encodable as uint16. UTF-8 well-formedness is also checked.
func ValidateTopicName(topic string) bool {
	if len(topic) == 0 || len(topic) > 65535 {
		return false
	}
	for i := 0; i < len(topic); {
		r, size := decodeRuneInString(topic[i:])
		if r == runeError || r == 0 {
			return false
		}
		if r == '+' || r == '#' {
			return false
		}
		i += size
	}
	return true
}

// ValidateFilter checks a subscription topic filter: same rules as a topic
// name, plus '+' and '#' may only occupy a whole level ('#' must be last).
func ValidateFilter(filter string) bool {
	if len(filter) == 0 || len(filter) > 65535 {
		return false
	}
	levels := splitLevels(filter)
	for i, level := range levels {
		if level == "#" {
			if i != len(levels)-1 {
				return false
			}
			continue
		}
		if level == "+" {
			continue
		}
		for j := 0; j < len(level); {
			r, size := decodeRuneInString(level[j:])
			if r == runeError || r == 0 || r == '+' || r == '#' {
				return false
			}
			j += size
		}
	}
	return true
}

// Match reports whether a concrete topic name matches a filter (MQTT 3.1.1
// section 4.7.1.2): '#' matches the rest, '+' matches one level, "$SYS"
// topics never match filters starting with a wildcard.
func Match(filter, topic string) bool {
	// Topics beginning with '$' (server control) are not matched by
	// wildcard filters.
	if len(topic) > 0 && topic[0] == '$' && (filter[0] == '+' || filter[0] == '#') {
		return false
	}
	f := splitLevels(filter)
	t := splitLevels(topic)
	for i, fl := range f {
		if fl == "#" {
			return true
		}
		if i >= len(t) {
			return false
		}
		if fl == "+" || fl == t[i] {
			continue
		}
		return false
	}
	return len(f) == len(t)
}

func splitLevels(s string) []string {
	var levels []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '/' {
			levels = append(levels, s[start:i])
			start = i + 1
		}
	}
	return append(levels, s[start:])
}
