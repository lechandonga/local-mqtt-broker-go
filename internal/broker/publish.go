package broker

import "github.com/lechandonga/local-mqtt-broker-go/internal/mqtt3"

func (c *client) handlePublish(pkt *mqtt3.Packet, sess *session) {
	r := mqtt3.NewReader(pkt.Variable)
	topic, err := r.String()
	if err != nil {
		c.protocolViolation("PUBLISH bad topic string")
		return
	}
	if err := mqtt3.ValidatePublishTopic(topic); err != nil {
		c.protocolViolation("PUBLISH invalid topic")
		return
	}
	qos := pkt.QoS()
	if qos == 3 {
		c.protocolViolation("PUBLISH QoS 3 reserved")
		return
	}

	var packetID uint16
	if qos > 0 {
		packetID, err = r.U16()
		if err != nil {
			c.protocolViolation("PUBLISH missing packet id")
			return
		}
		if packetID == 0 {
			c.protocolViolation("PUBLISH packet id must be nonzero")
			return
		}
	} else {
		if pkt.Flags&0x06 != 0 {
			c.protocolViolation("QoS0 PUBLISH must not carry QoS bits")
			return
		}
	}
	payload := r.Remaining()

	// Inbound QoS2 deduplication; QoS1 dup is accepted and forwarded again
	// (allowed by MQTT 3.1.1 §4.3.3 Dup re-delivery).
	if qos == 2 && packetID != 0 {
		c.broker.mu.Lock()
		dup := sess.inboundRel[packetID]
		seen := sess.inboundRec[packetID]
		if dup {
			// Retransmitted PUBLISH after PUBREL is illegal; client must
			// only retransmit before PUBREL.
			c.broker.mu.Unlock()
			c.protocolViolation("PUBLISH retransmitted after PUBREL")
			return
		}
		if !seen {
			sess.inboundRec[packetID] = true
		}
		c.broker.mu.Unlock()
		if seen {
			c.broker.cfg.Logger.Printf("event=dup_inbound client_id=%q packet_id=%d qos=2", c.clientID, packetID)
		}
	}

	switch qos {
	case 1:
		c.sendFixed(mqtt3.TypePUBACK, 0, packetID)
	case 2:
		c.sendFixed(mqtt3.TypePUBREC, 0, packetID)
	}

	if qos == 2 {
		// Hold application delivery until PUBREL: the message is not
		// published to subscribers yet (at-least-once-at-QoS2 handoff).
		c.broker.awaitPubRel(c, sess, packetID, topic, payload, pkt.Retain(), pkt.Dup())
		return
	}

	var retainByte byte
	if pkt.Retain() {
		retainByte = 1
	}
	c.broker.routePublish(topic, payload, qos, retainByte, c.clientID, false)
	c.broker.cfg.Logger.Printf("event=publish client_id=%q topic=%q qos=%d bytes=%d retain=%v",
		c.clientID, topic, qos, len(payload), pkt.Retain())
}

func (b *Broker) awaitPubRel(c *client, sess *session, id uint16, topic string, payload []byte, retain, dup bool) {
	// Record the message keyed on packet id; handleAck(PUBREL) publishes it.
	b.mu.Lock()
	sess.pendingRel = ensurePendingRel(sess.pendingRel)
	sess.pendingRel[id] = pendingRelMsg{topic: topic, payload: payload, retain: retain}
	b.mu.Unlock()
}

type pendingRelMsg struct {
	topic   string
	payload []byte
	retain  bool
}

func ensurePendingRel(m map[uint16]pendingRelMsg) map[uint16]pendingRelMsg {
	if m == nil {
		m = make(map[uint16]pendingRelMsg)
	}
	return m
}

func (c *client) handleAck(pkt *mqtt3.Packet, kind byte) {
	id, ok := u16id(pkt.Variable)
	if !ok || id == 0 {
		c.protocolViolation("ack packet missing packet id")
		return
	}
	b := c.broker
	b.mu.Lock()
	sess := b.sessions[c.clientID]
	switch kind {
	case mqtt3.TypePUBACK:
		// Completes an outbound QoS1 flow. Duplicate ACKs are idempotent.
		if f, ok := sess.outbound[id]; ok {
			if f.qos != 1 {
				b.mu.Unlock()
				c.protocolViolation("PUBACK for non-QoS1 packet id")
				return
			}
			delete(sess.outbound, id)
		}
	case mqtt3.TypePUBREC:
		if f, ok := sess.outbound[id]; ok {
			if f.qos != 2 {
				b.mu.Unlock()
				c.protocolViolation("PUBREC for non-QoS2 packet id")
				return
			}
			f.phase = 2
		}
		b.mu.Unlock()
		// Always answer PUBREL for PUBREC; duplicates re-answer harmlessly.
		c.sendFixed(mqtt3.TypePUBREL, 0x02, id)
		return
	case mqtt3.TypePUBREL:
		// Completes inbound QoS2: now deliver the application message.
		msg, waited := sess.pendingRel[id]
		if waited {
			delete(sess.pendingRel, id)
			delete(sess.inboundRec, id)
		}
		sess.inboundRel[id] = true
		b.mu.Unlock()
		if waited {
			var rb byte
			if msg.retain {
				rb = 1
			}
			b.routePublish(msg.topic, msg.payload, 2, rb, c.clientID, false)
		} else {
			b.cfg.Logger.Printf("event=dup_ack client_id=%q packet_id=%d type=PUBREL", c.clientID, id)
		}
		c.sendFixed(mqtt3.TypePUBCOMP, 0, id)
		return
	case mqtt3.TypePUBCOMP:
		if f, ok := sess.outbound[id]; ok {
			if f.qos != 2 {
				b.mu.Unlock()
				c.protocolViolation("PUBCOMP for non-QoS2 packet id")
				return
			}
			delete(sess.outbound, id)
		}
	}
	b.mu.Unlock()
}

// routePublish stores retained messages and fans a message out to subscribers.
// senderID is excluded per MQTT 3.1.1: a publisher also subscribed to its own
// topic DOES receive the message (no exclusion) — senderID is used only for
// will attribution logging.
func (b *Broker) routePublish(topic string, payload []byte, qos, retain byte, senderID string, isWill bool) {
	b.mu.Lock()
	if retain == 1 {
		b.storeRetained(topic, payload, qos)
	}
	hits := b.matchSubscribers(topic)
	targets := make([]struct {
		c   *client
		s   *session
		qos byte
	}, 0, len(hits))
	for id, gq := range hits {
		eff := qos
		if eff > gq {
			eff = gq
		}
		sess := b.sessions[id]
		cl := b.clients[id]
		if sess == nil {
			continue
		}
		targets = append(targets, struct {
			c   *client
			s   *session
			qos byte
		}{cl, sess, eff})
	}
	b.mu.Unlock()

	for _, t := range targets {
		b.forwardToClient(t.c, t.s, topic, payload, t.qos, false)
	}
	b.cfg.Logger.Printf("event=route topic=%q qos=%d retain=%d will=%v subscribers=%d",
		topic, qos, retain, isWill, len(targets))
	if retain == 1 && len(payload) == 0 {
		b.cfg.Logger.Printf("event=retain_clear topic=%q", topic)
	}
}

// forwardToClient delivers one message copy, honoring session semantics:
//   - connected QoS0: write immediately;
//   - connected QoS1/2: allocate packet id, persist as in-flight, write;
//   - offline durable session, QoS1/2: queue for redelivery after reconnect;
//   - offline clean session or QoS0: drop.
func (b *Broker) forwardToClient(cl *client, s *session, topic string, payload []byte, qos byte, asRetained bool) {
	if qos == 0 {
		if cl != nil {
			cl.sendPublish(topic, payload, 0, 0, false, asRetained)
		}
		return
	}

	b.mu.Lock()
	if cl == nil {
		if s.clean {
			b.mu.Unlock()
			b.cfg.Logger.Printf("event=drop_offline client_id=%q topic=%q qos=%d clean=true", s.clientID, topic, qos)
			return
		}
		if len(s.queue) >= b.cfg.MaxSessionQueue {
			b.mu.Unlock()
			b.cfg.Logger.Printf("event=queue_overflow client_id=%q topic=%q action=drop_oldest", s.clientID, topic)
			b.mu.Lock()
			s.queue = s.queue[1:]
		}
		cp := make([]byte, len(payload))
		copy(cp, payload)
		s.queue = append(s.queue, queuedMessage{topic: topic, payload: cp, qos: qos, retain: asRetained})
		b.mu.Unlock()
		return
	}

	id := s.allocPacketID()
	if id == 0 {
		b.mu.Unlock()
		b.cfg.Logger.Printf("event=packet_ids_exhausted client_id=%q action=queue", s.clientID)
		// Enqueue locally; the client receives it once ACKs free IDs.
		b.forwardToClient(nil, s, topic, payload, qos, asRetained)
		return
	}
	cp := make([]byte, len(payload))
	copy(cp, payload)
	s.outbound[id] = &inflightOut{topic: topic, payload: cp, qos: qos, retain: asRetained, phase: 1}
	b.mu.Unlock()

	cl.sendPublish(topic, payload, qos, id, false, asRetained)
}

func (c *client) sendPublish(topic string, payload []byte, qos byte, id uint16, dup, retain bool) {
	w := mqtt3.NewWriter()
	w.String(topic)
	if qos > 0 {
		w.U16(id)
	}
	w.Bytes(payload)
	var flags byte
	if dup {
		flags |= 0x08
	}
	flags |= qos << 1
	if retain {
		flags |= 0x01
	}
	c.enqueue((&mqtt3.Packet{Type: mqtt3.TypePUBLISH, Flags: flags, Variable: w.B}).Encode())
}
