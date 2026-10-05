package broker

import (
	"time"

	"github.com/lechandonga/local-mqtt-broker-go/internal/mqtt3"
)

// handleConnect parses and validates the CONNECT packet and establishes the
// session. Returns true when the connection loop must stop.
func (c *client) handleConnect(pkt *mqtt3.Packet) bool {
	r := mqtt3.NewReader(pkt.Variable)

	protoName, err := r.String()
	if err != nil || protoName != "MQTT" {
		// MQTT 3.1 uses "MQIsdp"; this broker supports 3.1.1 only.
		c.sendConnackSync(false, mqtt3.ConnRefusedBadProtocol)
		c.broker.cfg.Logger.Printf("remote=%s connect_reject=bad_protocol_name name=%q", c.conn.RemoteAddr(), protoName)
		c.terminate(reasonProtocolError)
		return true
	}
	level, err := r.Byte()
	if err != nil || level != 4 {
		c.sendConnackSync(false, mqtt3.ConnRefusedBadProtocol)
		c.broker.cfg.Logger.Printf("remote=%s connect_reject=bad_protocol_level level=%d", c.conn.RemoteAddr(), level)
		c.terminate(reasonProtocolError)
		return true
	}
	flags, err := r.Byte()
	if err != nil {
		c.protocolViolation("truncated connect flags")
		return true
	}
	if flags&0x01 != 0 {
		// Reserved bit must be 0 in 3.1.1.
		c.protocolViolation("CONNECT reserved flag bit set")
		return true
	}
	ka, err := r.U16()
	if err != nil {
		c.protocolViolation("truncated keepalive")
		return true
	}

	clean := flags&0x02 != 0
	willFlag := flags&0x04 != 0
	willQoS := (flags >> 3) & 0x03
	willRetain := flags&0x20 != 0
	userFlag := flags&0x80 != 0
	passFlag := flags&0x40 != 0

	if !willFlag && (willQoS != 0 || willRetain) {
		c.protocolViolation("will fields set without will flag")
		return true
	}
	if willQoS == 3 {
		c.protocolViolation("will QoS must be 0..2")
		return true
	}
	if passFlag && !userFlag {
		// Password without username is allowed by some brokers, but the flag
		// order is explicitly constrained in 3.1.1; treat as malformed.
		c.protocolViolation("password flag without username flag")
		return true
	}

	clientID, err := r.String()
	if err != nil {
		c.protocolViolation("bad client id string")
		return true
	}
	if clientID == "" && !clean {
		c.sendConnackSync(false, mqtt3.ConnRefusedIdentifierRejected)
		c.terminate(reasonProtocolError)
		return true
	}

	var will *willMessage
	if willFlag {
		wt, err := r.String()
		if err != nil {
			c.protocolViolation("bad will topic")
			return true
		}
		if err := mqtt3.ValidatePublishTopic(wt); err != nil {
			c.protocolViolation("invalid will topic")
			return true
		}
		wm, err := r.Binary()
		if err != nil {
			c.protocolViolation("bad will message")
			return true
		}
		will = &willMessage{topic: wt, payload: wm, qos: willQoS, retain: willRetain}
	}
	// Username/password are accepted but not authenticated in this local
	// broker; fields must still be structurally present when flagged.
	if userFlag {
		if _, err := r.String(); err != nil {
			c.protocolViolation("bad username string")
			return true
		}
	}
	if passFlag {
		if _, err := r.Binary(); err != nil {
			c.protocolViolation("bad password bytes")
			return true
		}
	}

	c.mu.Lock()
	c.clientID = clientID
	c.cleanSession = clean
	c.keepAlive = time.Duration(ka) * time.Second
	c.will = will
	c.mu.Unlock()

	sess, sessionPresent := c.broker.attachClient(c, clean)

	c.sendConnack(sessionPresent, mqtt3.ConnAccepted)
	c.broker.cfg.Logger.Printf("event=connect client_id=%q clean=%v session_present=%v keepalive=%ds will=%v remote=%s",
		clientID, clean, sessionPresent, ka, willFlag, c.conn.RemoteAddr())

	c.broker.resumeSession(c, sess)
	return false
}

func (c *client) sendConnack(sessionPresent bool, code byte) {
	c.enqueue(c.connackFrame(sessionPresent, code))
}

func (c *client) sendConnackSync(sessionPresent bool, code byte) {
	c.writeSync(c.connackFrame(sessionPresent, code))
}

func (c *client) connackFrame(sessionPresent bool, code byte) []byte {
	sp := byte(0)
	if sessionPresent {
		sp = 1
	}
	w := mqtt3.NewWriter().Byte(sp).Byte(code)
	return (&mqtt3.Packet{Type: mqtt3.TypeCONNACK, Variable: w.B}).Encode()
}

// attachClient registers the connection, taking over any existing one and
// creating or restoring the session. Broker mutex held semantics inside.
func (b *Broker) attachClient(c *client, clean bool) (*session, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if old, ok := b.clients[c.clientID]; ok {
		old.terminate(reasonTakeover)
	}

	var sess *session
	present := false
	if clean {
		if old, ok := b.sessions[c.clientID]; ok {
			// Clean session discards any durable state (MQTT-3.1.4-2).
			b.removeSubsLocked(old)
			delete(b.sessions, c.clientID)
		}
		sess = newSession(c.clientID)
		sess.clean = true
		b.sessions[c.clientID] = sess
	} else {
		sess, present = b.sessions[c.clientID]
		if !present {
			sess = newSession(c.clientID)
			sess.clean = false
			b.sessions[c.clientID] = sess
		}
	}
	b.clients[c.clientID] = c
	return sess, present
}

func (b *Broker) removeSubsLocked(s *session) {
	for f := range s.subs {
		b.trieRemove(levels(f), s.clientID)
	}
	s.subs = make(map[string]byte)
}
