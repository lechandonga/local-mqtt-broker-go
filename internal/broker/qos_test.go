package broker_test

import (
	"testing"
	"time"

	"github.com/lechandonga/local-mqtt-broker-go/internal/mqtt3"
	"github.com/lechandonga/local-mqtt-broker-go/internal/testclient"
)

func waitPublish(t *testing.T, c *testclient.Client) (string, []byte, *mqtt3.Packet) {
	t.Helper()
	p, err := c.ReadPacket(2 * time.Second)
	if err != nil {
		t.Fatalf("wait publish: %v", err)
	}
	if p.Type != mqtt3.TypePUBLISH {
		t.Fatalf("want PUBLISH, got type %d", p.Type)
	}
	topic, _, payload, err := testclient.ParsePublish(p)
	if err != nil {
		t.Fatal(err)
	}
	return topic, payload, p
}

func TestQoS0Fanout(t *testing.T) {
	env := newEnv(t)
	pub := mustConnect(t, env, "pub0", true)
	sub := mustConnect(t, env, "sub0", true)
	if _, err := sub.Subscribe("qos0/t", 0); err != nil {
		t.Fatal(err)
	}
	if p, err := sub.ReadPacket(time.Second); err != nil || p.Type != mqtt3.TypeSUBACK {
		t.Fatalf("suback: %v", err)
	}
	if _, err := pub.Publish("qos0/t", []byte("hello-0"), 0, false); err != nil {
		t.Fatal(err)
	}
	topic, payload, p := waitPublish(t, sub)
	if topic != "qos0/t" || string(payload) != "hello-0" || p.QoS() != 0 {
		t.Fatalf("unexpected publish: %q %q qos=%d", topic, payload, p.QoS())
	}
	caseLog("QoS0", "至多一次扇出", "PASS", "")
}

func TestQoS1AckAndRedelivery(t *testing.T) {
	env := newEnv(t)
	pub := mustConnect(t, env, "pub1", true)

	t.Run("持久订阅者断线期间缓存QoS1并重发", func(t *testing.T) {
		sub := mustConnect(t, env, "sub1-d", false)
		if _, err := sub.Subscribe("qos1/d", 1); err != nil {
			t.Fatal(err)
		}
		if p, _ := sub.ReadPacket(time.Second); p.Type != mqtt3.TypeSUBACK {
			t.Fatal("missing suback")
		}
		// Abnormal disconnect while offline messages should be queued.
		if err := sub.Kill(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)

		if _, err := pub.Publish("qos1/d", []byte("offline-1"), 1, false); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)

		sub2, err := testclient.Dial(env.b.Addr())
		if err != nil {
			t.Fatal(err)
		}
		sp, code, err := sub2.Connect(testclient.ConnectParams{ClientID: "sub1-d", CleanSession: false})
		if err != nil || code != 0 {
			t.Fatalf("reconnect code=%d err=%v", code, err)
		}
		if !sp {
			t.Fatal("sessionPresent should be true for durable session")
		}
		topic, payload, p := waitPublish(t, sub2)
		if topic != "qos1/d" || string(payload) != "offline-1" || p.QoS() != 1 {
			t.Fatalf("unexpected queued delivery: %q %q qos=%d", topic, payload, p.QoS())
		}
		_, id, _, _ := testclient.ParsePublish(p)

		// Do not ACK yet; disconnect again and reconnect: the broker must
		// redeliver the SAME id with DUP=1, not duplicate the message.
		if err := sub2.Kill(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
		sub3, err := testclient.Dial(env.b.Addr())
		if err != nil {
			t.Fatal(err)
		}
		_, _, _ = sub3.Connect(testclient.ConnectParams{ClientID: "sub1-d", CleanSession: false})
		_, payload2, p2 := waitPublish(t, sub3)
		_, id2, _, _ := testclient.ParsePublish(p2)
		if string(payload2) != "offline-1" || id2 != id || !p2.Dup() {
			t.Fatalf("redelivery mismatch: id %d vs %d dup=%v", id, id2, p2.Dup())
		}
		if err := sub3.Puback(id2); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
		sub3.Disconnect()
		caseLog("QoS1", "离线缓存与同ID DUP重发", "PASS", "")
	})

	t.Run("清洁会话离线消息丢弃", func(t *testing.T) {
		sub := mustConnect(t, env, "sub1-clean", true)
		if _, err := sub.Subscribe("qos1/c", 1); err != nil {
			t.Fatal(err)
		}
		if p, _ := sub.ReadPacket(time.Second); p.Type != mqtt3.TypeSUBACK {
			t.Fatal("missing suback")
		}
		_ = sub.Kill()
		time.Sleep(100 * time.Millisecond)
		_, _ = pub.Publish("qos1/c", []byte("gone"), 1, false)
		time.Sleep(100 * time.Millisecond)

		sub2 := mustConnect(t, env, "sub1-clean", true) // brand new clean session
		if _, err := sub2.Subscribe("qos1/c", 1); err != nil {
			t.Fatal(err)
		}
		pkts := drain(t, sub2, 300*time.Millisecond)
		for _, p := range pkts {
			if p.Type == mqtt3.TypePUBLISH {
				t.Fatal("clean session must not receive offline messages")
			}
		}
		caseLog("QoS1", "清洁会话离线丢弃", "PASS", "")
	})

	t.Run("重复PUBACK幂等无害", func(t *testing.T) {
		sub := mustConnect(t, env, "dupack", false)
		if _, err := sub.Subscribe("qos1/ack", 1); err != nil {
			t.Fatal(err)
		}
		_, _ = sub.ReadPacket(time.Second)
		_, _ = pub.Publish("qos1/ack", []byte("x"), 1, false)
		_, _, p := waitPublish(t, sub)
		_, id, _, _ := testclient.ParsePublish(p)
		if err := sub.Puback(id); err != nil {
			t.Fatal(err)
		}
		if err := sub.Puback(id); err != nil { // duplicate
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
		ping := sub.Ping()
		if ping != nil {
			t.Fatal(ping)
		}
		p, err := sub.ReadPacket(time.Second)
		if err != nil || p.Type != mqtt3.TypePINGRESP {
			t.Fatalf("connection should survive duplicate ack: %v", err)
		}
		caseLog("QoS1", "重复PUBACK幂等", "PASS", "")
	})
}

func TestQoS2Flow(t *testing.T) {
	env := newEnv(t)

	t.Run("完整入站QoS2四次握手", func(t *testing.T) {
		// Broker-as-server side: subscriber runs the outbound QoS2 state
		// machine against a publishing client... Here we test the broker's
		// INBOUND handling: client publishes QoS2 and must see PUBREC.
		pub := mustConnect(t, env, "q2pub", true)
		sub := mustConnect(t, env, "q2sub", true)
		_, _ = sub.Subscribe("q2/t", 2)
		p, _ := sub.ReadPacket(time.Second)
		if p.Type != mqtt3.TypeSUBACK || p.Variable[2] != 2 {
			t.Fatalf("suback=% x", p.Variable)
		}

		id, err := pub.Publish("q2/t", []byte("exactly-once"), 2, false)
		if err != nil {
			t.Fatal(err)
		}
		// Publisher side: PUBREC -> PUBREL -> PUBCOMP.
		p, err = pub.ReadPacket(2 * time.Second)
		if err != nil || p.Type != mqtt3.TypePUBREC {
			t.Fatalf("want PUBREC: %v %+v", err, p)
		}
		if err := pub.Pubrel(id); err != nil {
			t.Fatal(err)
		}
		p, err = pub.ReadPacket(2 * time.Second)
		if err != nil || p.Type != mqtt3.TypePUBCOMP {
			t.Fatalf("want PUBCOMP: %v %+v", err, p)
		}

		// Subscriber side: outbound QoS2, must answer PUBREC..PUBCOMP.
		topic, payload, sp := waitPublish(t, sub)
		sid := extractID(sp)
		if topic != "q2/t" || string(payload) != "exactly-once" {
			t.Fatalf("subscriber got %q %q", topic, payload)
		}
		if err := sub.Pubrec(sid); err != nil {
			t.Fatal(err)
		}
		pr, err := sub.ReadPacket(2 * time.Second)
		if err != nil || pr.Type != mqtt3.TypePUBREL {
			t.Fatalf("want PUBREL: %v %+v", err, pr)
		}
		if err := sub.Pubcomp(sid); err != nil {
			t.Fatal(err)
		}
		caseLog("QoS2", "PUBLISH/PUBREC/PUBREL/PUBCOMP", "PASS", "")
	})

	t.Run("重复入站QoS2 PUBLISH只应答不重复下发", func(t *testing.T) {
		pub := mustConnect(t, env, "q2dup", true)
		sub := mustConnect(t, env, "q2dupsub", true)
		_, _ = sub.Subscribe("q2/dup", 2)
		_, _ = sub.ReadPacket(time.Second)

		_ = pub.PublishWithID("q2/dup", []byte("once"), 2, 777, false, false)
		p, err := pub.ReadPacket(2 * time.Second)
		if err != nil || p.Type != mqtt3.TypePUBREC {
			t.Fatalf("pubrec: %v %+v", err, p)
		}
		// Retransmit identical PUBLISH before PUBREL: same PUBREC answer,
		// application message is still delivered once after PUBREL.
		_ = pub.PublishWithID("q2/dup", []byte("once"), 2, 777, true, false)
		p2, err := pub.ReadPacket(time.Second)
		if err != nil || p2.Type != mqtt3.TypePUBREC {
			t.Fatalf("dup pubrec: %v", err)
		}
		_ = pub.Pubrel(777)
		p3, _ := pub.ReadPacket(2 * time.Second)
		if p3.Type != mqtt3.TypePUBCOMP {
			t.Fatalf("pubcomp: %+v", p3)
		}
		_, payload, sp := waitPublish(t, sub)
		_ = sub.Pubrec(extractID(sp))
		pr, _ := sub.ReadPacket(2 * time.Second)
		_ = sub.Pubcomp(extractID(pr))
		if string(payload) != "once" {
			t.Fatal("payload mismatch")
		}
		// No second application message allowed.
		extra := drain(t, sub, 250*time.Millisecond)
		for _, e := range extra {
			if e.Type == mqtt3.TypePUBLISH {
				t.Fatal("duplicate application delivery on QoS2")
			}
		}
		caseLog("QoS2", "重复PUBLISH去重", "PASS", "")
	})

	t.Run("QoS降级: 订阅QoS0接收QoS2发布", func(t *testing.T) {
		pub := mustConnect(t, env, "q2downgradepub", true)
		sub := mustConnect(t, env, "q2downgradesub", true)
		_, _ = sub.Subscribe("q2/down", 0)
		_, _ = sub.ReadPacket(time.Second)
		id, _ := pub.Publish("q2/down", []byte("d"), 2, false)
		// Publisher's inbound QoS2 handshake must complete before the broker
		// releases the message to subscribers.
		pr, err := pub.ReadPacket(2 * time.Second)
		if err != nil || pr.Type != mqtt3.TypePUBREC {
			t.Fatalf("want PUBREC: %v %+v", err, pr)
		}
		_ = pub.Pubrel(id)
		pc, err := pub.ReadPacket(2 * time.Second)
		if err != nil || pc.Type != mqtt3.TypePUBCOMP {
			t.Fatalf("want PUBCOMP: %v %+v", err, pc)
		}
		_, _, p := waitPublish(t, sub)
		if p.QoS() != 0 {
			t.Fatalf("expected downgraded QoS0, got %d", p.QoS())
		}
		caseLog("QoS降级", "QoS2发布到QoS0订阅", "PASS", "")
	})
}

func extractID(p *mqtt3.Packet) uint16 {
	_, id, _, _ := testclient.ParsePublish(p)
	if id == 0 && len(p.Variable) >= 2 {
		id = uint16(p.Variable[0])<<8 | uint16(p.Variable[1])
	}
	return id
}
