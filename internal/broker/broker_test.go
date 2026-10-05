package broker_test

import (
	"bytes"
	"io"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/lechandonga/local-mqtt-broker-go/internal/broker"
	"github.com/lechandonga/local-mqtt-broker-go/internal/mqtt3"
	"github.com/lechandonga/local-mqtt-broker-go/internal/testclient"
)

// testEnv wires a broker with a log buffer mirrored to stdout so test runs
// show which business flows were exercised.
type testEnv struct {
	t      *testing.T
	b      *broker.Broker
	logBuf *threadSafeBuffer
}

type threadSafeBuffer struct {
	mu sync.Mutex
	bytes.Buffer
}

func (b *threadSafeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Write(p)
}

func (b *threadSafeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.String()
}

func newEnv(t *testing.T) *testEnv {
	t.Helper()
	buf := &threadSafeBuffer{}
	mw := io.MultiWriter(os.Stdout, buf)
	logger := log.New(mw, "  [broker] ", log.Lmicroseconds)
	b := broker.New(broker.Config{
		Addr:             "127.0.0.1:0",
		MaxSubscriptions: 8,
		MaxSessionQueue:  32,
		WriteQueueDepth:  64,
		Logger:           logger,
	})
	if err := b.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return &testEnv{t: t, b: b, logBuf: buf}
}

// caseLog prints a business/case/result line visible in go test output.
func caseLog(business, tc, result, detail string) {
	log.Printf("业务=%-16s 用例=%-28s 结果=%s %s", business, tc, result, detail)
}

func mustConnect(t *testing.T, env *testEnv, id string, clean bool) *testclient.Client {
	t.Helper()
	c, err := testclient.Dial(env.b.Addr())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	sp, code, err := c.Connect(testclient.ConnectParams{ClientID: id, CleanSession: clean, KeepAlive: 30})
	if err != nil || code != mqtt3.ConnAccepted {
		t.Fatalf("connect %s: code=%d err=%v", id, code, err)
	}
	if sp {
		t.Fatalf("fresh session must report sessionPresent=false")
	}
	return c
}

// expectClosed fails if the connection still delivers packets within the wait.
func expectClosed(t *testing.T, c *testclient.Client, wait time.Duration) {
	t.Helper()
	pkt, err := c.ReadPacket(wait)
	if err == nil {
		t.Fatalf("expected connection close, got packet type=%d", pkt.Type)
	}
}

// drain reads packets until timeout, returning the captured packets.
func drain(t *testing.T, c *testclient.Client, wait time.Duration) []*mqtt3.Packet {
	t.Helper()
	var out []*mqtt3.Packet
	for {
		p, err := c.ReadPacket(wait)
		if err != nil {
			return out
		}
		out = append(out, p)
	}
}
