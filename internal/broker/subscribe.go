package broker

import (
	"strings"

	"github.com/lechandonga/local-mqtt-broker-go/internal/mqtt3"
)

// SUBACK failure code for a rejected filter.
const subFailureCode byte = 0x80

func (c *client) handleSubscribe(pkt *mqtt3.Packet) {
	r := mqtt3.NewReader(pkt.Variable)
	packetID, err := r.U16()
	if err != nil || packetID == 0 {
		c.protocolViolation("SUBSCRIBE missing packet id")
		return
	}

	type sub struct {
		filter string
		qos    byte
	}
	var subs []sub
	body := r.Remaining()
	br := mqtt3.NewReader(body)
	for br.Pos() < len(br.B) {
		f, err := br.String()
		if err != nil {
			c.protocolViolation("SUBSCRIBE bad filter string")
			return
		}
		q, err := br.Byte()
		if err != nil {
			c.protocolViolation("SUBSCRIBE missing QoS byte")
			return
		}
		if q&0xFC != 0 || q == 3 {
			c.protocolViolation("SUBSCRIBE QoS byte must be 0..2")
			return
		}
		subs = append(subs, sub{f, q})
	}
	if len(subs) == 0 {
		c.protocolViolation("SUBSCRIBE must contain at least one filter")
		return
	}

	grants := make([]byte, len(subs))
	type replay struct {
		match retainedMatch
		qos   byte
	}
	var replays []replay

	b := c.broker
	b.mu.Lock()
	sess := b.sessions[c.clientID]
	for i, s := range subs {
		reason := ""
		if err := mqtt3.ValidateFilter(s.filter); err != nil {
			grants[i] = subFailureCode
			reason = "bad_filter"
		} else if n := strings.Count(s.filter, "/") + 1; n > b.cfg.MaxTopicLevels {
			grants[i] = subFailureCode
			reason = "too_many_levels"
		} else {
			if _, exists := sess.subs[s.filter]; !exists && len(sess.subs) >= b.cfg.MaxSubscriptions {
				grants[i] = subFailureCode
				reason = "subscription_limit"
			} else {
				sess.subs[s.filter] = s.qos
				node := b.trieWalkCreate(levels(s.filter))
				node.subs[c.clientID] = s.qos
				grants[i] = s.qos
			}
		}
		if reason != "" {
			b.cfg.Logger.Printf("event=subscribe_reject client_id=%q filter=%q reason=%s", c.clientID, s.filter, reason)
			continue
		}
		for _, m := range b.matchRetained(s.filter, s.qos) {
			replays = append(replays, replay{match: m, qos: s.qos})
		}
	}
	b.mu.Unlock()

	w := mqtt3.NewWriter().U16(packetID)
	for _, g := range grants {
		w.Byte(g)
	}
	c.enqueue((&mqtt3.Packet{Type: mqtt3.TypeSUBACK, Variable: w.B}).Encode())
	c.broker.cfg.Logger.Printf("event=subscribe client_id=%q filters=%d grants=%v", c.clientID, len(subs), grants)

	// Deliver matching retained messages (excluding empty-payload clears).
	for _, rp := range replays {
		m := rp.match
		if m.payload == nil {
			continue
		}
		q := m.qos
		if q > rp.qos {
			q = rp.qos
		}
		b.forwardToClient(c, sess, m.topic, m.payload, q, true)
	}
}

func (c *client) handleUnsubscribe(pkt *mqtt3.Packet) {
	r := mqtt3.NewReader(pkt.Variable)
	packetID, err := r.U16()
	if err != nil || packetID == 0 {
		c.protocolViolation("UNSUBSCRIBE missing packet id")
		return
	}
	var filters []string
	br := mqtt3.NewReader(r.Remaining())
	for br.Pos() < len(br.B) {
		f, err := br.String()
		if err != nil {
			c.protocolViolation("UNSUBSCRIBE bad filter string")
			return
		}
		filters = append(filters, f)
	}
	if len(filters) == 0 {
		c.protocolViolation("UNSUBSCRIBE must contain a filter")
		return
	}

	b := c.broker
	b.mu.Lock()
	sess := b.sessions[c.clientID]
	for _, f := range filters {
		if _, ok := sess.subs[f]; ok {
			delete(sess.subs, f)
			b.trieRemove(levels(f), c.clientID)
		}
	}
	b.mu.Unlock()

	c.enqueue((&mqtt3.Packet{
		Type:     mqtt3.TypeUNSUBACK,
		Variable: mqtt3.NewWriter().U16(packetID).B,
	}).Encode())
	c.broker.cfg.Logger.Printf("event=unsubscribe client_id=%q filters=%v", c.clientID, filters)
}
