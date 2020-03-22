package broker

import (
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/pkg/errors"
	_ "github.com/zfair/zqtt/internal/topic"
	"go.uber.org/zap"
)

func (c *Conn) messagePump(startedChan chan int) error {
	var err error

	flushTicker := time.NewTicker(c.FlushInterval)
	flushChan := flushTicker.C

	close(startedChan)

	for {
		select {
		case <-c.ExitChan:
			goto exit
		// TODO: 优化 flush，没有新数据无需 flush
		case <-flushChan:
			c.writerLock.Lock()
			err = c.Flush()
			c.writerLock.Unlock()
			if err != nil {
				goto exit
			}
		case msg := <-c.msgChan:
			c.writerLock.Lock()
			err = msg.Write(c.writer)
			c.writerLock.Unlock()
			if err != nil {
				goto exit
			}
		}
	}

exit:
	c.server.logger.Info("messagePump exit", zap.Uint64("luid", uint64(c.luid)))
	flushTicker.Stop()
	if err != nil {
		c.server.logger.Error(
			"messagePump exit",
			zap.Uint64("luid", uint64(c.luid)),
			zap.Error(err),
		)
	}
	return err
}

// TODO: code it
func (c *Conn) onPacket(packet packets.ControlPacket) error {
	var err error
	switch p := packet.(type) {
	case *packets.ConnectPacket:
		err = c.onConnect(p)
	default:
		err = errors.Errorf("unimplement")
	}
	return err
}

func (c *Conn) onConnect(packet *packets.ConnectPacket) error {
	connack := packets.NewControlPacket(
		packets.Connack,
	)
	return c.Send(connack)
}
