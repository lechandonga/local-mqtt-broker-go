// Package testclient is a minimal MQTT 3.1.1 client on real TCP used by the
// broker test suite. It is intentionally synchronous and small.
package testclient

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/lechandonga/local-mqtt-broker-go/internal/mqtt3"
)

type Client struct {
	conn net.Conn
	mu   sync.Mutex
	pid  uint16
}

// Dial opens a raw TCP connection without sending CONNECT.
func Dial(addr string) (*Client, error) {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn}, nil
}

// ConnectParams configures a CONNECT packet.
type ConnectParams struct {
	ClientID     string
	CleanSession bool
	KeepAlive    uint16
	WillTopic    string
	WillPayload  []byte
	WillQoS      byte
	WillRetain   bool
}

// Connect sends CONNECT and returns (sessionPresent, returnCode).
func (c *Client) Connect(p ConnectParams) (bool, byte, error) {
	w := mqtt3.NewWriter()
	w.String("MQTT")
	w.Byte(4)
	var flags byte
	if p.CleanSession {
		flags |= 0x02
	}
	if p.WillTopic != "" {
		flags |= 0x04
		flags |= (p.WillQoS & 0x03) << 3
		if p.WillRetain {
			flags |= 0x20
		}
	}
	w.Byte(flags)
	w.U16(p.KeepAlive)
	w.String(p.ClientID)
	if p.WillTopic != "" {
		w.String(p.WillTopic)
		w.Binary(p.WillPayload)
	}
	if err := c.write(mqtt3.TypeCONNECT, 0, w.B); err != nil {
		return false, 0, err
	}
	pkt, err := c.ReadPacket(2 * time.Second)
	if err != nil {
		return false, 0, err
	}
	if pkt.Type != mqtt3.TypeCONNACK || len(pkt.Variable) < 2 {
		return false, 0, fmt.Errorf("expected CONNACK, got %+v", pkt)
	}
	return pkt.Variable[0] == 1, pkt.Variable[1], nil
}

func (c *Client) write(t, flags byte, body []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := &mqtt3.Packet{Type: t, Flags: flags, Variable: body}
	_, err := c.conn.Write(p.Encode())
	return err
}

// ReadPacket reads one packet with an optional deadline (0 = no deadline).
func (c *Client) ReadPacket(timeout time.Duration) (*mqtt3.Packet, error) {
	if timeout > 0 {
		_ = c.conn.SetReadDeadline(time.Now().Add(timeout))
	} else {
		_ = c.conn.SetReadDeadline(time.Time{})
	}
	return mqtt3.ReadPacket(c.conn, mqtt3.DefaultMaxPacketSize)
}

// Publish sends a PUBLISH and returns the packet id for QoS 1/2.
func (c *Client) Publish(topic string, payload []byte, qos byte, retain bool) (uint16, error) {
	w := mqtt3.NewWriter()
	w.String(topic)
	var id uint16
	if qos > 0 {
		id = c.nextID()
		w.U16(id)
	}
	w.Bytes(payload)
	var flags byte = qos << 1
	if retain {
		flags |= 1
	}
	return id, c.write(mqtt3.TypePUBLISH, flags, w.B)
}

// PublishWithID sends a PUBLISH with explicit packet id and DUP flag.
func (c *Client) PublishWithID(topic string, payload []byte, qos byte, id uint16, dup, retain bool) error {
	w := mqtt3.NewWriter()
	w.String(topic)
	if qos > 0 {
		w.U16(id)
	}
	w.Bytes(payload)
	var flags byte = qos << 1
	if dup {
		flags |= 0x08
	}
	if retain {
		flags |= 1
	}
	return c.write(mqtt3.TypePUBLISH, flags, w.B)
}

// Subscribe sends SUBSCRIBE and returns the packet id.
func (c *Client) Subscribe(filter string, qos byte) (uint16, error) {
	id := c.nextID()
	w := mqtt3.NewWriter().U16(id)
	w.String(filter)
	w.Byte(qos)
	return id, c.write(mqtt3.TypeSUBSCRIBE, 0x02, w.B)
}

// SubscribeMany sends one SUBSCRIBE with multiple filters.
func (c *Client) SubscribeMany(filters []string, qos []byte) (uint16, error) {
	id := c.nextID()
	w := mqtt3.NewWriter().U16(id)
	for i, f := range filters {
		w.String(f)
		w.Byte(qos[i])
	}
	return id, c.write(mqtt3.TypeSUBSCRIBE, 0x02, w.B)
}

func (c *Client) Unsubscribe(filter string) (uint16, error) {
	id := c.nextID()
	w := mqtt3.NewWriter().U16(id)
	w.String(filter)
	return id, c.write(mqtt3.TypeUNSUBSCRIBE, 0x02, w.B)
}

func (c *Client) Puback(id uint16) error {
	return c.write(mqtt3.TypePUBACK, 0, mqtt3.NewWriter().U16(id).B)
}
func (c *Client) Pubrec(id uint16) error {
	return c.write(mqtt3.TypePUBREC, 0, mqtt3.NewWriter().U16(id).B)
}
func (c *Client) Pubrel(id uint16) error {
	return c.write(mqtt3.TypePUBREL, 0x02, mqtt3.NewWriter().U16(id).B)
}
func (c *Client) Pubcomp(id uint16) error {
	return c.write(mqtt3.TypePUBCOMP, 0, mqtt3.NewWriter().U16(id).B)
}

func (c *Client) Ping() error {
	return c.write(mqtt3.TypePINGREQ, 0, nil)
}

// Disconnect sends MQTT DISCONNECT and closes the socket gracefully.
func (c *Client) Disconnect() error {
	err := c.write(mqtt3.TypeDISCONNECT, 0, nil)
	_ = c.conn.Close()
	return err
}

// Kill closes the TCP socket without DISCONNECT (abnormal disconnect).
func (c *Client) Kill() error { return c.conn.Close() }

// Raw writes arbitrary bytes, for malformed-packet tests.
func (c *Client) Raw(b []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := c.conn.Write(b)
	return err
}

func (c *Client) nextID() uint16 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pid++
	if c.pid == 0 {
		c.pid = 1
	}
	return c.pid
}

// ParsePublish extracts topic, packet id (QoS1/2) and payload from a PUBLISH.
func ParsePublish(p *mqtt3.Packet) (topic string, id uint16, payload []byte, err error) {
	r := mqtt3.NewReader(p.Variable)
	topic, err = r.String()
	if err != nil {
		return
	}
	if p.QoS() > 0 {
		id, err = r.U16()
		if err != nil {
			return
		}
	}
	payload = r.Remaining()
	return
}
