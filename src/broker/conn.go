package broker

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/zfair/zqtt/src/internal/topic"
	"github.com/zfair/zqtt/src/internal/util"
	"github.com/zfair/zqtt/src/zerr"
	"github.com/zfair/zqtt/src/zqttpb"
)

const defaultBufferSize = 16 * 1024
const defaultPollSubscribeMessageInterval = 5 * time.Second

// Conn is the broker connection.
type Conn struct {
	socket net.Conn

	// Reading/writing interfaces
	reader *bufio.Reader
	writer *bufio.Writer

	// Meta mutext
	MetaLock sync.Mutex
	// Writer mutex
	writerLock sync.Mutex

	HeartbeatTimeout             time.Duration
	FlushInterval                time.Duration
	PollSubscribeMessageInterval time.Duration

	ExitChan chan int
	sendChan chan []byte

	username string // The username provided by the client during MQTT connect.
	clientID string // The client id provided by the client during MQTT connect.

	luid uint64 // local unique id of this connection
	guid string // global unique id of this connection

	connected int32 // atomic operation on this field
	server    *Server

	subTopics     sync.Map // save subscribed topic for this connection
	messageIDRing *MessageIDRing
}

func newConn(s *Server, socket net.Conn) (*Conn, error) {
	uid, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}

	s.incrConnCount()

	return &Conn{
		socket: socket,

		reader: bufio.NewReaderSize(socket, defaultBufferSize),
		writer: bufio.NewWriterSize(socket, defaultBufferSize),

		HeartbeatTimeout:             s.getCfg().HeartbeatTimeout / 2,
		FlushInterval:                s.getCfg().FlushInterval,
		PollSubscribeMessageInterval: defaultPollSubscribeMessageInterval,

		ExitChan: make(chan int),
		sendChan: make(chan []byte),

		luid: util.NewLUID(),
		guid: uid.String(),

		connected:     0,
		server:        s,
		messageIDRing: NewMessageIDRing(),
	}, nil
}

// LUID (local UID) of a specific connection.
func (c *Conn) LUID() uint64 {
	return c.luid
}

// IOLoop for a upcoming connection.
func (c *Conn) IOLoop(ctx context.Context) error {
	messagePumpStarted := make(chan int)
	messagePumpErrChan := make(chan error)

	go func() {
		err := c.messagePump(messagePumpStarted)
		messagePumpErrChan <- err
	}()

	// Wait until the message pump started
	<-messagePumpStarted

	var zeroTime time.Time
	var err error

	for {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			goto exit
		case err = <-messagePumpErrChan:
			goto exit
		default:
			if c.HeartbeatTimeout > 0 {
				_ = c.socket.SetReadDeadline(time.Now().Add(c.HeartbeatTimeout))
			} else {
				_ = c.socket.SetReadDeadline(zeroTime)
			}
			var packet packets.ControlPacket
			packet, err = packets.ReadPacket(c.reader)
			if err != nil {
				if err == io.EOF {
					err = nil
				}
				goto exit
			}
			err = c.onPacket(ctx, packet)
			if err != nil {
				goto exit
			}
		}
	}

exit:
	c.server.logger.Info("IOLoop exits", zap.Uint64("luid", uint64(c.luid)))
	if err != nil {
		c.server.logger.Error(
			"IOLoop exits",
			zap.Uint64("luid", uint64(c.luid)),
			zap.Error(err),
		)
	}
	close(c.ExitChan)
	err = c.Close()
	if err != nil {
		c.server.logger.Error(
			"IOLoop closed",
			zap.Uint64("luid", uint64(c.luid)),
			zap.Error(err),
		)
	}

	return errors.WithStack(err)
}

func (c *Conn) setConnected(username string, clientID string) {
	c.MetaLock.Lock()
	c.username = username
	c.clientID = clientID
	c.MetaLock.Unlock()

	atomic.StoreInt32(&c.connected, 1)
}

func (c *Conn) isConnected() bool {
	return atomic.LoadInt32(&c.connected) == 1
}

func (c *Conn) GetUsername() string {
	return c.username
}

func (c *Conn) GetClientID() string {
	return c.clientID
}

// SendMessage sends only a *publish* message to the client.
func (c *Conn) SendMessage(ctx context.Context, msg *zqttpb.Message) error {
	messageID, err := c.messageIDRing.GetID()
	if err != nil {
		return errors.WithStack(err)
	}
	packet := (packets.NewControlPacket(packets.Publish)).(*packets.PublishPacket)
	packet.MessageID = messageID
	packet.Qos = byte(msg.Qos)
	packet.TopicName = msg.TopicName
	packet.Payload = msg.Payload
	buf := new(bytes.Buffer)
	err = packet.Write(buf)
	if err != nil {
		return errors.WithStack(err)
	}
	return c.Send(ctx, buf.Bytes())
}

// Send data to the peer.
func (c *Conn) Send(ctx context.Context, b []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case c.sendChan <- b:
		return nil
	case <-c.ExitChan:
		return zerr.ErrConnClosed
	}
}

// Close the connection.
func (c *Conn) Close() error {
	err := c.socket.Close()
	c.subTopics.Range(func(k interface{}, v interface{}) bool {
		ssid := v.([]uint64)
		c.server.logger.Debug(
			"[Conn] Close Unsubscribe topic",
			zap.String("topic", k.(string)),
		)
		err := c.server.subTrie.Unsubscribe(ssid, c)
		// TODO: report error
		_ = err
		return true
	})
	return errors.WithStack(err)
}

// Flush the send buffer.
func (c *Conn) Flush() error {
	var zeroTime time.Time
	if c.HeartbeatTimeout > 0 {
		_ = c.socket.SetWriteDeadline(time.Now().Add(c.HeartbeatTimeout))
	} else {
		_ = c.socket.SetWriteDeadline(zeroTime)
	}

	err := c.writer.Flush()
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (c *Conn) ID() uint64 {
	return c.luid
}

func (c *Conn) Kind() topic.SubscriberKind {
	return topic.SubscriberKindLocal
}

func (c *Conn) StoreSubTopic(ctx context.Context, topicName string, ssid []uint64) {
	c.subTopics.Store(topicName, ssid)
}

func (c *Conn) DeleteSubTopic(ctx context.Context, topicName string) {
	c.subTopics.Delete(topicName)
}
