package broker

import "github.com/lechandonga/local-mqtt-broker-go/internal/mqtt3"

// resumeSession replays unacknowledged outbound flows and drains the offline
// queue after a durable session reconnects. Called with the client attached.
func (b *Broker) resumeSession(c *client, s *session) {
	b.mu.Lock()
	ids := make([]uint16, 0, len(s.outbound))
	for id := range s.outbound {
		ids = append(ids, id)
	}
	queued := s.queue
	s.queue = nil
	b.mu.Unlock()

	for _, id := range ids {
		b.mu.Lock()
		f := s.outbound[id]
		b.mu.Unlock()
		if f == nil {
			continue
		}
		if f.phase == 2 && f.qos == 2 {
			// Re-send PUBREL; the PUBLISH itself is not repeated.
			c.sendFixed(mqtt3.TypePUBREL, 0x02, id)
		} else {
			c.sendPublish(f.topic, f.payload, f.qos, id, true, f.retain)
		}
		b.cfg.Logger.Printf("event=redeliver client_id=%q packet_id=%d phase=%d", c.clientID, id, f.phase)
	}
	for _, m := range queued {
		b.forwardToClient(c, s, m.topic, m.payload, m.qos, m.retain)
	}
}

// cleanup runs once when the connection ends. It publishes the will (unless
// the client sent DISCONNECT), detaches the socket and handles session state.
func (c *client) cleanup() {
	c.closeOnce.Do(func() {
		c.reason = reasonNetworkIO
		close(c.doneCh)
		_ = c.conn.Close()
	})

	c.mu.Lock()
	id := c.clientID
	clean := c.cleanSession
	will := c.will
	graceful := c.reason == reasonClientDISCONNECT
	c.mu.Unlock()

	if id == "" {
		c.broker.cfg.Logger.Printf("event=disconnect remote=%s reason=%s", c.conn.RemoteAddr(), c.reason)
		return
	}

	b := c.broker
	b.mu.Lock()
	if b.clients[id] == c {
		delete(b.clients, id)
	}

	sess := b.sessions[id]
	if clean {
		if sess != nil {
			b.removeSubsLocked(sess)
			delete(b.sessions, id)
		}
	} else if sess != nil {
		// Inbound partial QoS2 flows: an ungraceful disconnect discards
		// PUBREC-only state so the publisher may safely retransmit the
		// PUBLISH; PUBREL-acknowledged (awaiting PUBCOMP) state is kept so
		// we can still answer a retransmitted PUBREL.
		sess.inboundRec = make(map[uint16]bool)
	}
	b.mu.Unlock()

	if !graceful && will != nil {
		c.broker.cfg.Logger.Printf("event=will client_id=%q reason=%s topic=%q", id, c.reason, will.topic)
		var rb byte
		if will.retain {
			rb = 1
		}
		b.routePublish(will.topic, will.payload, will.qos, rb, id, true)
	}

	c.broker.cfg.Logger.Printf("event=disconnect client_id=%q reason=%s clean=%v", id, c.reason, clean)
}

// sendFixed sends PUBACK/PUBREC/PUBREL/PUBCOMP carrying only a packet id.
func (c *client) sendFixed(t, flags byte, id uint16) {
	w := mqtt3.NewWriter().U16(id)
	c.enqueue((&mqtt3.Packet{Type: t, Flags: flags, Variable: w.B}).Encode())
}
