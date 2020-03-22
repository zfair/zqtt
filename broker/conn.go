package broker

import (
	"bufio"
	"io"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/zfair/zqtt/internal/util"
	"github.com/zfair/zqtt/zerrors"
	"go.uber.org/zap"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

const defaultBufferSize = 16 * 1024

type Conn struct {
	socket net.Conn

	// reading/writing interfaces
	reader *bufio.Reader
	writer *bufio.Writer

	// writer mutex
	writerLock sync.Mutex

	// heart timeout
	HeartbeatTimeout time.Duration
	// flush interval
	FlushInterval time.Duration

	// ExitChan
	ExitChan chan int
	// Message Channel
	msgChan chan packets.ControlPacket

	username string // The username provided by the client during MQTT connect.
	luid     uint64 // local unique id of this connection
	guid     string // global uinque id of this connection

	server *Server
}

func newConn(s *Server, socket net.Conn) (*Conn, error) {
	uuid, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}

	s.incConnections()

	return &Conn{
		socket: socket,

		reader: bufio.NewReaderSize(socket, defaultBufferSize),
		writer: bufio.NewWriterSize(socket, defaultBufferSize),

		HeartbeatTimeout: s.getCfg().HeartbeatTimeout / 2,
		FlushInterval:    s.getCfg().FlushInterval,

		ExitChan: make(chan int),
		msgChan:  make(chan packets.ControlPacket),

		luid:   util.NewLUID(),
		guid:   uuid.String(),
		server: s,
	}, nil
}

func (c *Conn) LUID() uint64 {
	return c.luid
}

func (c *Conn) IOLoop() error {
	messagePumpStarted := make(chan int)
	messagePumpErrChan := make(chan error)
	go func() {
		err := c.messagePump(messagePumpStarted)
		messagePumpErrChan <- err
	}()
	// wait until the message pump started
	<-messagePumpStarted

	var zeroTime time.Time
	var err error
	for {
		select {
		case err = <-messagePumpErrChan:
			goto exit
		default:
			if c.HeartbeatTimeout > 0 {
				c.socket.SetReadDeadline(time.Now().Add(c.HeartbeatTimeout))
			} else {
				c.socket.SetReadDeadline(zeroTime)
			}
			var packet packets.ControlPacket
			packet, err = packets.ReadPacket(c.reader)
			if err != nil {
				if err == io.EOF {
					err = nil
				}
				goto exit
			}
			err = c.onPacket(packet)
			if err != nil {
				goto exit
			}
		}
	}
exit:
	c.server.logger.Info("IOLoop exit", zap.Uint64("luid", uint64(c.luid)))
	if err != nil {
		c.server.logger.Error(
			"IOLoop exit",
			zap.Uint64("luid", uint64(c.luid)),
			zap.Error(err),
		)
	}
	close(c.ExitChan)
	err = c.Close()
	if err != nil {
		c.server.logger.Error(
			"IOLoop exit close",
			zap.Uint64("luid", uint64(c.luid)),
			zap.Error(err),
		)
	}
	return err
}

func (c Conn) Send(msg packets.ControlPacket) error {
	select {
	case c.msgChan <- msg:
		return nil
	case <-c.ExitChan:
		return zerrors.ErrConnExited
	}
}

func (c *Conn) Close() error {
	err := c.socket.Close()
	return err
}

func (c *Conn) Flush() error {
	var zeroTime time.Time
	if c.HeartbeatTimeout > 0 {
		c.socket.SetWriteDeadline(time.Now().Add(c.HeartbeatTimeout))
	} else {
		c.socket.SetWriteDeadline(zeroTime)
	}

	err := c.writer.Flush()
	if err != nil {
		return err
	}

	return nil
}
