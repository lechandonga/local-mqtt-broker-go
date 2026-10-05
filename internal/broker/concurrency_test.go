package broker_test

import (
	"sync"
	"testing"
	"time"

	"github.com/lechandonga/local-mqtt-broker-go/internal/mqtt3"
	"github.com/lechandonga/local-mqtt-broker-go/internal/testclient"
)

// TestManyDevicesFaninFanout simulates several devices publishing to a shared
// telemetry topic while a gateway receives every message over real TCP.
func TestManyDevicesFaninFanout(t *testing.T) {
	env := newEnv(t)

	const deviceCount = 8
	const msgsPerDevice = 6

	gw := mustConnect(t, env, "gateway", false)
	if _, err := gw.Subscribe("telemetry/+/up", 1); err != nil {
		t.Fatal(err)
	}
	subAck, err := gw.ReadPacket(time.Second)
	if err != nil || subAck.Type != mqtt3.TypeSUBACK {
		t.Fatal("gateway suback")
	}

	// Single dedicated reader goroutine owns socket reads and auto-ACKs QoS1;
	// the main goroutine only consumes from a channel.
	type inMsg struct{ payload []byte }
	inbox := make(chan inMsg, deviceCount*msgsPerDevice*2)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			p, err := gw.ReadPacket(0)
			if err != nil {
				return
			}
			if p.Type == mqtt3.TypePUBLISH {
				_, _, payload, perr := testclient.ParsePublish(p)
				if perr != nil {
					continue
				}
				id := extractID(p)
				if p.QoS() == 1 {
					_ = gw.Puback(id)
				}
				select {
				case inbox <- inMsg{payload}:
				default:
				}
			}
		}
	}()

	var wg sync.WaitGroup
	for d := 0; d < deviceCount; d++ {
		wg.Add(1)
		go func(d int) {
			defer wg.Done()
			dev := mustConnect(t, env, deviceName(d), true)
			for m := 0; m < msgsPerDevice; m++ {
				topic := "telemetry/" + deviceName(d) + "/up"
				if _, err := dev.Publish(topic, []byte{byte(d), byte(m)}, 1, false); err != nil {
					t.Errorf("device %d publish: %v", d, err)
					return
				}
			}
			// Drain all PUBACKs after publishing (incoming broker traffic
			// only, and read lock no longer contends with writes).
			for m := 0; m < msgsPerDevice; m++ {
				ack, err := dev.ReadPacket(2 * time.Second)
				if err != nil {
					t.Errorf("device %d ack: %v", d, err)
					return
				}
				if ack.Type != mqtt3.TypePUBACK {
					t.Errorf("device %d expected PUBACK, got %d", d, ack.Type)
					return
				}
			}
			_ = dev.Disconnect()
		}(d)
	}

	received := make(map[byte]int)
	deadline := time.After(10 * time.Second)
	for total := 0; total < deviceCount*msgsPerDevice; {
		select {
		case <-deadline:
			t.Fatalf("timeout: received %d/%d, per-device=%v", total, deviceCount*msgsPerDevice, received)
		case m := <-inbox:
			received[m.payload[0]]++
			total++
		}
	}
	wg.Wait()

	for d := 0; d < deviceCount; d++ {
		if received[byte(d)] != msgsPerDevice {
			t.Fatalf("device %d delivered %d, want %d", d, received[byte(d)], msgsPerDevice)
		}
	}
	caseLog("多设备并发", "8设备x6消息QoS1汇聚", "PASS", "网关收到48/48且逐包ACK")
}

func deviceName(d int) string {
	return "dev-" + string(rune('a'+d))
}
