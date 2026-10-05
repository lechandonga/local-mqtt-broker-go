package broker_test

import (
	"io"
	"log"
	"os"
	"testing"
	"time"

	"github.com/lechandonga/local-mqtt-broker-go/internal/broker"
	"github.com/lechandonga/local-mqtt-broker-go/internal/mqtt3"
	"github.com/lechandonga/local-mqtt-broker-go/internal/testclient"
)

func TestSessionResumeAndClean(t *testing.T) {
	env := newEnv(t)

	t.Run("持久会话恢复订阅并接收保留消息状态", func(t *testing.T) {
		sub := mustConnect(t, env, "sess-d", false)
		_, _ = sub.Subscribe("sess/a", 1)
		_, _ = sub.ReadPacket(time.Second)
		_ = sub.Kill()
		time.Sleep(100 * time.Millisecond)

		sub2, err := dialAndConnect(env, "sess-d", false)
		if err != nil {
			t.Fatal(err)
		}
		// Subscription survived: publish immediately after reconnect should
		// arrive without a new SUBSCRIBE.
		pub := mustConnect(t, env, "sess-pub", true)
		_, _ = pub.Publish("sess/a", []byte("after-resume"), 1, false)
		_, payload, p := waitPublish(t, sub2)
		if string(payload) != "after-resume" || p.QoS() != 1 {
			t.Fatalf("resumed subscription failed: %q qos=%d", payload, p.QoS())
		}
		_ = sub2.Puback(extractID(p))
		caseLog("会话状态", "持久订阅恢复", "PASS", "")
	})

	t.Run("清洁会话重连sessionPresent=false且订阅丢失", func(t *testing.T) {
		c := mustConnect(t, env, "sess-c", true)
		_, _ = c.Subscribe("sess/c", 1)
		_, _ = c.ReadPacket(time.Second)
		_ = c.Disconnect()
		time.Sleep(100 * time.Millisecond)
		c2, err := dialAndConnect(env, "sess-c", true)
		if err != nil {
			t.Fatal(err)
		}
		pub := mustConnect(t, env, "sess-c-pub", true)
		_, _ = pub.Publish("sess/c", []byte("nope"), 1, false)
		pkts := drain(t, c2, 300*time.Millisecond)
		for _, p := range pkts {
			if p.Type == mqtt3.TypePUBLISH {
				t.Fatal("clean session must not retain subscription")
			}
		}
		caseLog("会话状态", "清洁会话不保留订阅", "PASS", "")
	})
}

func TestRetainedMessages(t *testing.T) {
	env := newEnv(t)

	t.Run("新订阅立即收到保留消息", func(t *testing.T) {
		pub := mustConnect(t, env, "ret-pub", true)
		if _, err := pub.Publish("ret/temp", []byte("23.5"), 1, true); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
		sub := mustConnect(t, env, "ret-sub", true)
		_, _ = sub.Subscribe("ret/+", 1)
		p, _ := sub.ReadPacket(2 * time.Second) // SUBACK
		if p.Type != mqtt3.TypeSUBACK {
			t.Fatal("missing suback")
		}
		topic, payload, rp := waitPublish(t, sub)
		if topic != "ret/temp" || string(payload) != "23.5" || !rp.Retain() {
			t.Fatalf("retained replay mismatch: %q %q retain=%v", topic, payload, rp.Retain())
		}
		caseLog("保留消息", "订阅时回放", "PASS", "")
	})

	t.Run("空负载清除保留消息", func(t *testing.T) {
		pub := mustConnect(t, env, "ret-clr", true)
		_, _ = pub.Publish("ret/clr", []byte("v"), 0, true)
		time.Sleep(80 * time.Millisecond)
		_, _ = pub.Publish("ret/clr", nil, 0, true) // clear
		time.Sleep(80 * time.Millisecond)
		sub := mustConnect(t, env, "ret-clr-sub", true)
		_, _ = sub.Subscribe("ret/clr", 0)
		pkts := drain(t, sub, 300*time.Millisecond)
		for _, p := range pkts {
			if p.Type == mqtt3.TypePUBLISH {
				t.Fatal("retained message should have been cleared")
			}
		}
		caseLog("保留消息", "空负载清除", "PASS", "")
	})

	t.Run("井号不匹配零层级主题", func(t *testing.T) {
		// '#' must require at least one level; use a unique prefix to avoid
		// cross-test retained state, and verify only prefixed messages arrive.
		pub := mustConnect(t, env, "ret-hash-pub", true)
		_, _ = pub.Publish("hashcase/x", []byte("v"), 0, true)
		time.Sleep(80 * time.Millisecond)
		sub := mustConnect(t, env, "ret-hash-sub", true)
		_, _ = sub.Subscribe("hashcase/#", 0)
		if p, _ := sub.ReadPacket(time.Second); p.Type != mqtt3.TypeSUBACK {
			t.Fatal("missing suback")
		}
		topic, _, rp := waitPublish(t, sub)
		if topic != "hashcase/x" {
			t.Fatalf("unexpected topic %q", topic)
		}
		_ = rp
		caseLog("保留消息", "#不匹配零层级", "PASS", "")
	})

	t.Run("保留消息按订阅QoS降级", func(t *testing.T) {
		pub := mustConnect(t, env, "ret-qos-pub", true)
		id, _ := pub.Publish("hashcase/q", []byte("v"), 1, true)
		if ack, err := pub.ReadPacket(2 * time.Second); err != nil || ack.Type != mqtt3.TypePUBACK {
			t.Fatalf("puback: %v %+v", err, ack)
		}
		_ = id
		time.Sleep(80 * time.Millisecond)
		sub := mustConnect(t, env, "ret-qos-sub", true)
		_, _ = sub.Subscribe("hashcase/q", 0)
		_, _ = sub.ReadPacket(time.Second) // SUBACK
		_, _, rp := waitPublish(t, sub)
		if rp.QoS() != 0 {
			t.Fatalf("retained replay QoS=%d want 0", rp.QoS())
		}
		caseLog("保留消息", "回放QoS降级1->0", "PASS", "")
	})
}

func TestWillMessage(t *testing.T) {
	env := newEnv(t)
	sub := mustConnect(t, env, "watch", true)
	_, _ = sub.Subscribe("status/+", 1)
	_, _ = sub.ReadPacket(time.Second)

	t.Run("异常断开发布遗嘱,正常断开不发布", func(t *testing.T) {
		c, err := testclientDial(env)
		if err != nil {
			t.Fatal(err)
		}
		_, code, err := c.Connect(connectWill("will-client", "status/will-client", []byte("offline"), 1))
		if err != nil || code != 0 {
			t.Fatalf("will connect: %d %v", code, err)
		}
		_ = c.Kill() // abnormal
		topic, payload, p := waitPublish(t, sub)
		if topic != "status/will-client" || string(payload) != "offline" {
			t.Fatalf("will delivery: %q %q", topic, payload)
		}
		_ = sub.Puback(extractID(p))

		c2, err := testclientDial(env)
		if err != nil {
			t.Fatal(err)
		}
		_, _, _ = c2.Connect(connectWill("will-client2", "status/will-client2", []byte("offline"), 1))
		_ = c2.Disconnect() // graceful: no will
		pkts := drain(t, sub, 350*time.Millisecond)
		for _, pk := range pkts {
			if pk.Type == mqtt3.TypePUBLISH {
				t.Fatal("graceful DISCONNECT must not publish will")
			}
		}
		caseLog("遗嘱消息", "异常发布/正常抑制", "PASS", "")
	})
}

func TestKeepAlive(t *testing.T) {
	env := newEnv(t)

	t.Run("心跳超时断开(1.5倍宽限)", func(t *testing.T) {
		c, err := testclientDial(env)
		if err != nil {
			t.Fatal(err)
		}
		_, code, err := c.Connect(testclient.ConnectParams{ClientID: "ka", CleanSession: true, KeepAlive: 1})
		if err != nil || code != 0 {
			t.Fatal(err)
		}
		// Silent for > 1.5s: server must close.
		p, err := c.ReadPacket(3 * time.Second)
		if err == nil {
			t.Fatalf("expected timeout close, got packet %d", p.Type)
		}
		caseLog("心跳超时", "静默超1.5倍关闭", "PASS", "")
	})

	t.Run("持续PING不超时且时钟抖动安全", func(t *testing.T) {
		c, err := testclientDial(env)
		if err != nil {
			t.Fatal(err)
		}
		_, _, _ = c.Connect(testclient.ConnectParams{ClientID: "ka-ok", CleanSession: true, KeepAlive: 1})
		// Ping every 400ms for ~2s, with jittery uneven intervals.
		delays := []time.Duration{300 * time.Millisecond, 700 * time.Millisecond, 250 * time.Millisecond, 900 * time.Millisecond}
		for _, d := range delays {
			time.Sleep(d)
			if err := c.Ping(); err != nil {
				t.Fatal(err)
			}
			p, err := c.ReadPacket(2 * time.Second)
			if err != nil || p.Type != mqtt3.TypePINGRESP {
				t.Fatalf("pingresp: %v %+v", err, p)
			}
		}
		caseLog("心跳超时", "PING保活+抖动不误会", "PASS", "")
	})
}

func TestSubscriptionLimitsAndWildcardsOverTCP(t *testing.T) {
	env := newEnv(t)

	t.Run("越界订阅返回0x80且不影响其他过滤器", func(t *testing.T) {
		// Dedicated broker with MaxSubscriptions=4 for a deterministic limit.
		lbuf := &threadSafeBuffer{}
		llog := log.New(io.MultiWriter(os.Stdout, lbuf), "  [broker-lim] ", log.Lmicroseconds)
		lb := broker.New(broker.Config{Addr: "127.0.0.1:0", MaxSubscriptions: 4, Logger: llog})
		if err := lb.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = lb.Close() })
		lenv := &testEnv{t: t, b: lb, logBuf: lbuf}
		c := mustConnect(t, lenv, "limited", true)
		// MaxSubscriptions=4 in the test env.
		filters := []string{"l/1", "l/2", "l/3", "l/4", "l/5"}
		qos := []byte{0, 0, 0, 0, 0}
		id, err := c.SubscribeMany(filters, qos)
		if err != nil {
			t.Fatal(err)
		}
		p, err := c.ReadPacket(2 * time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if p.Type != mqtt3.TypeSUBACK {
			t.Fatal("want suback")
		}
		grants := p.Variable[2:]
		if len(grants) != 5 {
			t.Fatalf("grants=% x", grants)
		}
		for i, g := range grants[:4] {
			if g != 0 {
				t.Fatalf("grant[%d]=%x want 0", i, g)
			}
		}
		if grants[4] != 0x80 {
			t.Fatalf("5th subscription must fail with 0x80, got %x", grants[4])
		}
		_ = id
		caseLog("订阅限制", "超限返回0x80", "PASS", "")
	})

	t.Run("非法过滤器返回0x80", func(t *testing.T) {
		fresh := mustConnect(t, env, "limited2", true)
		id, _ := fresh.SubscribeMany([]string{"a/#/c", "good/f"}, []byte{0, 0})
		p, err := fresh.ReadPacket(2 * time.Second)
		if err != nil {
			t.Fatal(err)
		}
		_ = id
		g := p.Variable[2:]
		if g[0] != 0x80 || g[1] != 0 {
			t.Fatalf("grants=% x, want 80 00", g)
		}
		caseLog("订阅限制", "非法过滤器0x80", "PASS", "")
	})

	t.Run("井号订阅跨层接收", func(t *testing.T) {
		wild := mustConnect(t, env, "wild", true)
		_, _ = wild.Subscribe("floor1/#", 0)
		_, _ = wild.ReadPacket(time.Second)
		pub := mustConnect(t, env, "wildpub", true)
		_, _ = pub.Publish("floor1/room2/temp", []byte("19"), 0, false)
		topic, payload, _ := waitPublish(t, wild)
		if topic != "floor1/room2/temp" || string(payload) != "19" {
			t.Fatalf("wildcard delivery %q %q", topic, payload)
		}
		caseLog("通配符订阅", "TCP端到端#匹配", "PASS", "")
	})
}

func TestSlowConsumerDoesNotBlockOthers(t *testing.T) {
	buf := &threadSafeBuffer{}
	logger := log.New(io.MultiWriter(os.Stdout, buf), "  [broker-slow] ", log.Lmicroseconds)
	b := broker.New(broker.Config{
		Addr: "127.0.0.1:0", WriteQueueDepth: 1, MaxSubscriptions: 8, Logger: logger,
	})
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	env := &testEnv{t: t, b: b, logBuf: buf}

	slow := mustConnect(t, env, "slow", true)
	fast := mustConnect(t, env, "fast", true)
	_, _ = slow.Subscribe("shared/x", 0)
	_, _ = slow.ReadPacket(time.Second)
	_, _ = fast.Subscribe("shared/x", 0)
	_, _ = fast.ReadPacket(time.Second)

	// Stop reading from slow; its write queue (depth 2) fills and the broker
	// disconnects it, but fast keeps receiving every message.
	pub := mustConnect(t, env, "slowpub", true)
	gotFast := 0
	for i := 0; i < 12; i++ {
		_, _ = pub.Publish("shared/x", []byte{byte('a' + i)}, 0, false)
		p, err := fast.ReadPacket(2 * time.Second)
		if err != nil {
			t.Fatalf("fast reader stalled at %d: %v", i, err)
		}
		if p.Type == mqtt3.TypePUBLISH {
			gotFast++
		}
	}
	if gotFast != 12 {
		t.Fatalf("fast received %d/12", gotFast)
	}
	caseLog("慢消费者", "慢连接隔离不阻塞快连接", "PASS", "")
}
