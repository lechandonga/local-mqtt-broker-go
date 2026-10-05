package broker_test

import (
	"github.com/lechandonga/local-mqtt-broker-go/internal/testclient"
)

func dialAndConnect(env *testEnv, id string, clean bool) (*testclient.Client, error) {
	c, err := testclient.Dial(env.b.Addr())
	if err != nil {
		return nil, err
	}
	_, code, err := c.Connect(testclient.ConnectParams{ClientID: id, CleanSession: clean, KeepAlive: 30})
	if err != nil {
		return nil, err
	}
	if code != 0 {
		return nil, errConnCode(code)
	}
	return c, nil
}

type connCodeErr byte

func (e connCodeErr) Error() string {
	return "connect rejected with code " + string([]byte{'0' + byte(e)})
}

func errConnCode(code byte) error { return connCodeErr(code) }

func testclientDial(env *testEnv) (*testclient.Client, error) {
	return testclient.Dial(env.b.Addr())
}

func connectWill(id, topic string, payload []byte, qos byte) testclient.ConnectParams {
	return testclient.ConnectParams{
		ClientID:     id,
		CleanSession: true,
		KeepAlive:    30,
		WillTopic:    topic,
		WillPayload:  payload,
		WillQoS:      qos,
	}
}
