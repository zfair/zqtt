package broker

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/zfair/zqtt/src/internal/topic"
	"github.com/zfair/zqtt/src/internal/util"
	"github.com/zfair/zqtt/src/zerr"
)

const (
	connStateInit = iota + 1
	connStateDisconnected
	connStateConnected
	connStateClosing
	connStateClosed
)

const defaultBufferSize = 16 * 1024

// Conn is the broker connection.
type Conn struct {
	socket net.Conn

	// Reading/writing interfaces
	reader *bufio.Reader
	writer *bufio.Writer

	// Writer mutex
	writerLock sync.Mutex

	HeartbeatTimeout time.Duration
	FlushInterval    time.Duration

	ExitChan chan int
	sendChan chan []byte

	username string // The username provided by the client during MQTT connect.
	clientID string // The client id provided by the client during MQTT connect.

	luid uint64 // local unique id of this connection
	guid string // global unique id of this connection

	state  int32
	server *Server
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

		HeartbeatTimeout: s.getCfg().HeartbeatTimeout / 2,
		FlushInterval:    s.getCfg().FlushInterval,

		ExitChan: make(chan int),
		sendChan: make(chan []byte),

		luid: util.NewLUID(),
		guid: uid.String(),

		state:  connStateInit,
		server: s,
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

	return err
}

func (c Conn) setConnected(username string, clientID string) {
	c.state = connStateConnected
	c.username = username
	c.clientID = clientID
}

// SendMessage sends only a *publish* message to the client.
func (c Conn) SendMessage(ctx context.Context, msg *topic.Message) error {
	packet := (packets.NewControlPacket(packets.Publish)).(*packets.PublishPacket)
	packet.MessageID = msg.MessageID
	packet.Qos = msg.Qos
	packet.TopicName = msg.TopicName
	packet.Payload = msg.Payload
	buf := new(bytes.Buffer)
	err := packet.Write(buf)
	if err != nil {
		return err
	}
	return c.Send(ctx, buf.Bytes())
}

// Send data to the peer.
func (c Conn) Send(ctx context.Context, b []byte) error {
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
	return err
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
		return err
	}

	return nil
}
