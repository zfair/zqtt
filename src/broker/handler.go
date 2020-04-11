package broker

import (
	"bytes"
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/zfair/zqtt/src/zerr"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/zfair/zqtt/src/internal/topic"
)

var MaxTime time.Time = time.Unix(1<<63-1, 0)
var ZeroTime time.Time

func (c *Conn) messagePump(startedChan chan int) error {
	var err error

	flushTicker := time.NewTicker(c.FlushInterval)
	flushChan := flushTicker.C

	close(startedChan)

	for {
		select {
		case <-c.ExitChan:
			goto exit
		// TODO(locustchen): 优化 flush, 没有新数据无需 flush
		case <-flushChan:
			c.writerLock.Lock()
			err = c.Flush()
			c.writerLock.Unlock()
			if err != nil {
				goto exit
			}
		case b := <-c.sendChan:
			c.writerLock.Lock()
			_, err = c.writer.Write(b)
			c.writerLock.Unlock()
			if err != nil {
				goto exit
			}
		}
	}

exit:
	c.server.logger.Info("messagePump exits", zap.Uint64("luid", uint64(c.luid)))
	flushTicker.Stop()
	if err != nil {
		c.server.logger.Error(
			"messagePump exits",
			zap.Uint64("luid", uint64(c.luid)),
			zap.Error(err),
		)
	}
	return err
}

// TODO(locustchen)
func (c *Conn) onPacket(ctx context.Context, packet packets.ControlPacket) error {
	var err error
	switch p := packet.(type) {
	case *packets.ConnectPacket:
		err = c.onConnect(ctx, p)
	case *packets.PublishPacket:
		err = c.onPublish(ctx, p)
	default:
		err = errors.Errorf("unimplemented")
	}
	return err
}

func (c *Conn) onConnect(ctx context.Context, packet *packets.ConnectPacket) error {
	username := packet.Username
	clientID := packet.ClientIdentifier

	// TODO: add hooks function for connection auth and extension
	c.setConnected(username, clientID)

	connAck := packets.NewControlPacket(
		packets.Connack,
	)
	// TODO(locustchen): use buffer pool
	buf := new(bytes.Buffer)
	err := connAck.Write(buf)
	if err != nil {
		return err
	}
	return c.Send(ctx, buf.Bytes())
}

func (c *Conn) onPublish(ctx context.Context, packet *packets.PublishPacket) error {
	if !c.isConnected() {
		return zerr.ErrNotConnectd
	}

	topicName := packet.TopicName
	parser := topic.NewParser(topicName)
	parsedTopic, err := parser.Parse()
	if err != nil {
		return err
	}
	if parsedTopic.Kind() != topic.TopicKindStatic {
		return errors.Errorf("Invalid Publish Topic %s", topicName)
	}
	ssid := parsedTopic.ToSSID()
	// TODO: add hooks function for publish auth and extension
	uid, err := uuid.NewRandom()
	if err != nil {
		return err
	}
	m := topic.NewMessage(
		uid.String(),
		c.clientID,
		packet.MessageID,
		topicName,
		ssid,
		packet.Qos,
		ZeroTime,
		packet.Payload,
	)

	// TODO: Read ttl Options From Topic Name
	if packet.Retain {
		m.TTLUntil = MaxTime
	}
	// store message if needed
	if m.TTLUntil != ZeroTime {
		err := c.server.store.Store(ctx, m)
		if err != nil {
			return err
		}
	}
	subscribers := c.server.subTrie.Lookup(ssid)
	for _, subscriber := range subscribers {
		// ignore sendMessage error
		err := subscriber.SendMessage(ctx, m)
		if err != nil {
			c.server.logger.Info(
				"SendMessage Failed",
				zap.String("ClientID", c.clientID),
				zap.String("TopicName", topicName),
				zap.Uint64("SubscriberID", subscriber.ID()),
			)
		}
	}

	return nil
}
