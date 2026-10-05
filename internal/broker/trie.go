package broker

import "strings"

// trieNode stores subscriptions and retained messages by topic level.
// It is guarded by the broker mutex.
type trieNode struct {
	children map[string]*trieNode
	plus     *trieNode
	hash     *trieNode

	// subscriptions at this exact path: client ID -> QoS grant.
	subs map[string]byte

	// retained message stored at this exact path.
	retPayload []byte
	retQoS     byte
}

func newTrieNode() *trieNode {
	return &trieNode{children: make(map[string]*trieNode), subs: make(map[string]byte)}
}

func (n *trieNode) countSubs() int {
	total := len(n.subs)
	for _, c := range n.children {
		total += c.countSubs()
	}
	if n.plus != nil {
		total += n.plus.countSubs()
	}
	if n.hash != nil {
		total += n.hash.countSubs()
	}
	return total
}

func levels(topic string) []string { return strings.Split(topic, "/") }

func (b *Broker) trieWalkCreate(levels []string) *trieNode {
	node := b.subRoot
	for _, lv := range levels {
		switch lv {
		case "+":
			if node.plus == nil {
				node.plus = newTrieNode()
			}
			node = node.plus
		case "#":
			if node.hash == nil {
				node.hash = newTrieNode()
			}
			node = node.hash
		default:
			c := node.children[lv]
			if c == nil {
				c = newTrieNode()
				node.children[lv] = c
			}
			node = c
		}
	}
	return node
}

func (b *Broker) trieRemove(levels []string, clientID string) {
	// Small broker: prune lazily by clearing the subscription; empty nodes
	// are harmless and bounded by MaxSubscriptions over time plus reconnects.
	node := b.subRoot
	for _, lv := range levels {
		switch lv {
		case "+":
			node = node.plus
		case "#":
			node = node.hash
		default:
			node = node.children[lv]
		}
		if node == nil {
			return
		}
	}
	delete(node.subs, clientID)
}

// matchEntry is a subscriber hit while matching a published topic.
type matchEntry struct {
	clientID string
	qos      byte
}

// matchSubscribers collects every subscription matching topic. The same client
// may appear once with the maximum granted QoS across overlapping filters.
func (b *Broker) matchSubscribers(topic string) map[string]byte {
	out := make(map[string]byte)
	tl := levels(topic)
	var walk func(node *trieNode, i int)
	walk = func(node *trieNode, i int) {
		if node == nil {
			return
		}
		for id, q := range node.subs {
			if cur, ok := out[id]; !ok || q > cur {
				out[id] = q
			}
		}
		if i == len(tl) {
			return
		}
		// '#' at any reached node matches this level and all deeper levels.
		if node.hash != nil {
			for id, q := range node.hash.subs {
				if cur, ok := out[id]; !ok || q > cur {
					out[id] = q
				}
			}
		}
		walk(node.plus, i+1)
		walk(node.children[tl[i]], i+1)
	}
	walk(b.subRoot, 0)
	return out
}

// retainedMatch holds a retained message hit for a new subscription.
type retainedMatch struct {
	topic   string
	payload []byte
	qos     byte
}

// matchRetained returns retained messages visible under filter, at the
// effective QoS min(retained, granted).
func (b *Broker) matchRetained(filter string, grantedQoS byte) []retainedMatch {
	var out []retainedMatch
	fl := levels(filter)
	var walk func(node *trieNode, i int, path []string)
	walk = func(node *trieNode, i int, path []string) {
		if node == nil {
			return
		}
		if i == len(fl) {
			if node != b.retRoot && node.retPayload != nil {
				q := node.retQoS
				if q > grantedQoS {
					q = grantedQoS
				}
				p := make([]byte, len(node.retPayload))
				copy(p, node.retPayload)
				out = append(out, retainedMatch{
					topic:   strings.Join(path, "/"),
					payload: p,
					qos:     q,
				})
			}
			return
		}
		switch fl[i] {
		case "#":
			if node != b.retRoot {
				// '#' matches the current level ("a/#" matches "a")...
				collectRetained(node, path, &out)
			}
			// ...and all levels beneath it.
			for name, c := range node.children {
				next := append(append([]string(nil), path...), name)
				collectRetained(c, next, &out)
			}
		case "+":
			// Match any concrete child at this level. The retained trie
			// contains concrete levels only (no wildcard nodes).
			for name, c := range node.children {
				next := append(append([]string(nil), path...), name)
				walk(c, i+1, next)
			}
		default:
			next := append(append([]string(nil), path...), fl[i])
			walk(node.children[fl[i]], i+1, next)
		}
	}
	walk(b.retRoot, 0, nil)
	return out
}

func collectRetained(node *trieNode, path []string, out *[]retainedMatch) {
	if node == nil {
		return
	}
	if node.retPayload != nil {
		p := make([]byte, len(node.retPayload))
		copy(p, node.retPayload)
		*out = append(*out, retainedMatch{
			topic:   strings.Join(path, "/"),
			payload: p,
			qos:     node.retQoS,
		})
	}
	for name, c := range node.children {
		next := append(append([]string(nil), path...), name)
		collectRetained(c, next, out)
	}
}

func (b *Broker) storeRetained(topic string, payload []byte, qos byte) {
	node := b.retRoot
	for _, lv := range levels(topic) {
		c := node.children[lv]
		if c == nil {
			c = newTrieNode()
			node.children[lv] = c
		}
		node = c
	}
	if len(payload) == 0 {
		node.retPayload = nil // clear retained message
		node.retQoS = 0
		return
	}
	p := make([]byte, len(payload))
	copy(p, payload)
	node.retPayload = p
	node.retQoS = qos
}
