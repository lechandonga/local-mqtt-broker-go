package broker_test

import (
	"testing"
	"time"

	"github.com/lechandonga/local-mqtt-broker-go/internal/mqtt3"
	"github.com/lechandonga/local-mqtt-broker-go/internal/testclient"
)

func TestConnectValidation(t *testing.T) {
	env := newEnv(t)

	t.Run("错误协议名被拒绝0x01", func(t *testing.T) {
		c, err := testclient.Dial(env.b.Addr())
		if err != nil {
			t.Fatal(err)
		}
		// Hand-craft CONNECT with protocol name "MQTT5" (invalid).
		w := mqtt3.NewWriter()
		w.String("MQTT5")
		w.Byte(4).Byte(0x02).U16(30).String("badproto")
		if err := c.Raw((&mqtt3.Packet{Type: mqtt3.TypeCONNECT, Variable: w.B}).Encode()); err != nil {
			t.Fatal(err)
		}
		p, err := c.ReadPacket(2 * time.Second)
		if err != nil {
			t.Fatalf("read connack: %v", err)
		}
		if p.Type != mqtt3.TypeCONNACK || p.Variable[1] != mqtt3.ConnRefusedBadProtocol {
			t.Fatalf("want bad protocol connack, got %+v", p)
		}
		expectClosed(t, c, time.Second)
		caseLog("连接校验", "错误协议名被拒绝0x01", "PASS", "")
	})

	t.Run("空ClientID且非清洁会话返回0x02", func(t *testing.T) {
		c, err := testclient.Dial(env.b.Addr())
		if err != nil {
			t.Fatal(err)
		}
		_, code, err := c.Connect(testclient.ConnectParams{ClientID: "", CleanSession: false})
		if err != nil {
			t.Fatal(err)
		}
		if code != mqtt3.ConnRefusedIdentifierRejected {
			t.Fatalf("code=%d want 0x02", code)
		}
		expectClosed(t, c, time.Second)
		caseLog("连接校验", "空ClientID持久会话0x02", "PASS", "")
	})

	t.Run("空ClientID清洁会话接受", func(t *testing.T) {
		c, err := testclient.Dial(env.b.Addr())
		if err != nil {
			t.Fatal(err)
		}
		_, code, err := c.Connect(testclient.ConnectParams{ClientID: "", CleanSession: true})
		if err != nil || code != 0 {
			t.Fatalf("code=%d err=%v", code, err)
		}
		c.Disconnect()
		caseLog("连接校验", "空ClientID清洁会话接受", "PASS", "")
	})

	t.Run("保留位非法直接关闭", func(t *testing.T) {
		c, err := testclient.Dial(env.b.Addr())
		if err != nil {
			t.Fatal(err)
		}
		w := mqtt3.NewWriter()
		w.String("MQTT")
		w.Byte(4).Byte(0x03).U16(30).String("x1")
		if err := c.Raw((&mqtt3.Packet{Type: mqtt3.TypeCONNECT, Variable: w.B}).Encode()); err != nil {
			t.Fatal(err)
		}
		expectClosed(t, c, time.Second)
		caseLog("连接校验", "保留位非法直接关闭", "PASS", "")
	})

	t.Run("CONNECT前发其他报文被断开", func(t *testing.T) {
		c, err := testclient.Dial(env.b.Addr())
		if err != nil {
			t.Fatal(err)
		}
		_ = c.Ping()
		expectClosed(t, c, time.Second)
		caseLog("连接校验", "CONNECT前报文被拒", "PASS", "")
	})

	t.Run("同ID重连踢掉旧连接", func(t *testing.T) {
		old := mustConnect(t, env, "takeover", true)
		time.Sleep(100 * time.Millisecond)
		// clean=true: fresh session even though the old client is still attached.
		new := mustConnect(t, env, "takeover", true)
		expectClosed(t, old, time.Second)
		new.Disconnect()
		caseLog("连接校验", "同ID重连接管", "PASS", "")
	})
}

func TestMalformedPacketsIsolated(t *testing.T) {
	env := newEnv(t)
	good := mustConnect(t, env, "healthy", true)

	t.Run("畸形报文关闭单连接且不影响他人", func(t *testing.T) {
		bad := mustConnect(t, env, "bad", true)
		// Fault 1: reserved flags wrong on SUBSCRIBE (0x00 instead of 0x02).
		w := mqtt3.NewWriter().U16(1)
		w.String("a/b")
		w.Byte(0)
		_ = bad.Raw((&mqtt3.Packet{Type: mqtt3.TypeSUBSCRIBE, Flags: 0x00, Variable: w.B}).Encode())
		expectClosed(t, bad, time.Second)

		// Fault 2 on a different bad socket: truncated varint body.
		bad2 := mustConnect(t, env, "bad2", true)
		_ = bad2.Raw([]byte{0x30, 0x0A, 0x00, 0x01})
		expectClosed(t, bad2, 2*time.Second)

		// The unrelated "healthy" connection must keep serving normally.
		subID, _ := good.Subscribe("iso/test", 0)
		p, err := good.ReadPacket(2 * time.Second)
		if err != nil || p.Type != mqtt3.TypeSUBACK {
			t.Fatalf("healthy sub failed: %v %+v", err, p)
		}
		if uint16(p.Variable[0])<<8|uint16(p.Variable[1]) != subID {
			t.Fatalf("suback id mismatch")
		}
		caseLog("畸形报文", "单连接故障隔离", "PASS", "")
	})
}
