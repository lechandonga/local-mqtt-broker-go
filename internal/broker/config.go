package broker

import "time"

// Config holds broker resource limits. Zero values fall back to defaults.
type Config struct {
	// MaxClientIDLen limits the MQTT client identifier. MQTT 3.1.1 allows up
	// to 65535 chars; a smaller local default bounds per-connection memory.
	MaxClientIDLen int
	// MaxPacketSize caps one complete control packet (default 1 MiB).
	MaxPacketSize int
	// MaxInflight caps unacknowledged outbound QoS 1/2 messages per
	// persistent session (MQTT 3.1.1 §4.9, suggested minimum 65535).
	MaxInflight int
	// MaxQueuedMessages bounds offline + inflight-waiting QoS 1/2 messages
	// per session. Overflow drops the oldest queued message.
	MaxQueuedMessages int
	// KeepAliveGrace is added to 1.5*KeepAlive before a connection is
	// declared dead. It absorbs scheduler/clock jitter without weakening the
	// MQTT timeout rule.
	KeepAliveGrace time.Duration
	// WriteTimeout bounds each individual socket write.
	WriteTimeout time.Duration
}

func (c *Config) withDefaults() Config {
	d := *c
	if d.MaxClientIDLen <= 0 {
		d.MaxClientIDLen = 256
	}
	if d.MaxPacketSize <= 0 {
		d.MaxPacketSize = 1 << 20
	}
	if d.MaxInflight <= 0 {
		d.MaxInflight = 64
	}
	if d.MaxQueuedMessages <= 0 {
		d.MaxQueuedMessages = 1024
	}
	if d.KeepAliveGrace <= 0 {
		d.KeepAliveGrace = 500 * time.Millisecond
	}
	if d.WriteTimeout <= 0 {
		d.WriteTimeout = 10 * time.Second
	}
	return d
}
