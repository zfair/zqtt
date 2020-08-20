package broker

import (
	"bytes"
	"context"
	"time"

	"github.com/zfair/zqtt/src/zerr"
	"github.com/zfair/zqtt/src/zqttpb"

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
	return errors.WithStack(err)
}

// TODO(locustchen)
func (c *Conn) onPacket(ctx context.Context, packet packets.ControlPacket) error {
	var err error
	switch p := packet.(type) {
	case *packets.ConnectPacket:
		err = c.onConnect(ctx, p)
	case *packets.PublishPacket:
		err = c.onPublish(ctx, p)
	case *packets.PubackPacket:
		err = c.onPuback(ctx, p)
	case *packets.SubscribePacket:
		err = c.onSubscribe(ctx, p)

	default:
		c.server.logger.Error(
			"[Conn] onPacket unimplemented",
			zap.String("packet", packet.String()),
		)
		err = errors.Errorf("unimplemented")
	}
	return errors.WithStack(err)
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
		return errors.WithStack(err)
	}
	return c.Send(ctx, buf.Bytes())
}

func (c *Conn) onPublish(ctx context.Context, packet *packets.PublishPacket) error {
	if !c.isConnected() {
		return zerr.ErrNotConnectd
	}

	c.server.logger.Debug(
		"[Broker] OnPublish",
		zap.String("TopicName", packet.TopicName),
		zap.String("Username", c.GetUsername()),
		zap.Uint16("MessageID", packet.MessageID),
		zap.Int("RemainingLength", packet.RemainingLength),
	)

	// parse topic
	topicName := packet.TopicName
	parser := topic.NewParser(topicName)
	parsedTopic, err := parser.Parse()
	if err != nil {
		return errors.WithStack(err)
	}
	if parsedTopic.Kind() != topic.TopicKindStatic {
		return errors.Errorf("Invalid Publish Topic %s", topicName)
	}
	ssid := parsedTopic.ToSSID()
	// unmarshal message from payload
	m := zqttpb.Message{}
	err = m.Unmarshal(packet.Payload)
	if err != nil {
		return errors.WithStack(err)
	}

	if m.Username != c.GetUsername() {
		return zerr.ErrMismatchMessageUsername
	}

	if m.ClientID != c.GetClientID() {
		return zerr.ErrMismatchMessageClientID
	}

	if m.TopicName != packet.TopicName {
		return zerr.ErrMismatchMessageTopic
	}

	if byte(m.Qos) != packet.Qos {
		return zerr.ErrMismatchMessageQos
	}

	m.CreatedAt = time.Now().UnixNano()
	m.Ssid = ssid

	// Generate Message Seq
	seq, err := c.server.MSeqGeneator.GenMessageSeq(ctx, parsedTopic)
	if err != nil {
		return errors.WithStack(err)
	}
	m.MessageSeq = seq

	// always store message
	err = c.server.MStore.StoreMessage(ctx, &m)
	if err != nil {
		return errors.WithStack(err)
	}

	c.server.logger.Debug(
		"[Broker] OnPublish",
		zap.Any("clientID", c.GetClientID()),
		zap.Int64("messageSeq", m.MessageSeq),
	)

	subscribers := c.server.subTrie.Lookup(ssid)
	for _, subscriber := range subscribers {
		// ignore sendMessage error
		// because client side will poll message
		err := subscriber.SendMessage(ctx, &m)
		if err != nil {
			c.server.logger.Info(
				"[Broker] SendMessage Failed",
				zap.String("ClientID", c.clientID),
				zap.String("TopicName", topicName),
				zap.Uint64("SubscriberID", subscriber.ID()),
				zap.Error(err),
			)
		}
	}

	if packet.Qos > 0 {
		pubAck := packets.NewControlPacket(
			packets.Puback,
		).(*packets.PubackPacket)
		pubAck.MessageID = packet.MessageID

		buf := new(bytes.Buffer)
		err := pubAck.Write(buf)
		if err != nil {
			return errors.WithStack(err)
		}
		return c.Send(ctx, buf.Bytes())
	}

	return nil
}

func (c *Conn) onSubscribe(ctx context.Context, packet *packets.SubscribePacket) error {
	c.server.logger.Debug(
		"[Broker] onSubscribe",
		zap.Any("packet", packet),
	)
	if len(packet.Topics) != 1 {
		// simplify onSubscribe logic
		// only allow subscribe one topic per packet
		c.server.logger.Error(
			"[Broker] onSubscribe Length of topics != 1",
			zap.Uint64("luid", c.ID()),
		)
		subAck := packets.NewControlPacket(
			packets.Suback,
		).(*packets.SubackPacket)

		subAck.MessageID = packet.MessageID
		subAck.ReturnCodes = []byte{
			packets.ErrProtocolViolation,
		}
		buf := new(bytes.Buffer)
		err := subAck.Write(buf)
		if err != nil {
			return errors.WithStack(err)
		}
		return c.Send(ctx, buf.Bytes())
	}

	topicName := packet.Topics[0]
	parser := topic.NewParser(topicName)
	parsedTopic, err := parser.Parse()
	if err != nil {
		return errors.WithStack(err)
	}

	// store subscription to sstorage
	err = c.server.SStore.StoreSubscription(
		ctx,
		c.clientID,
		parsedTopic,
	)
	if err != nil {
		return errors.WithStack(err)
	}

	ssid := parsedTopic.ToSSID()
	err = c.server.subTrie.Subscribe(ssid, c)
	if err != nil {
		return errors.WithStack(err)
	}
	c.StoreSubTopic(ctx, topicName, ssid)
	// TODO(locustchen): use buffer pool
	subAck := packets.NewControlPacket(
		packets.Suback,
	).(*packets.SubackPacket)

	subAck.MessageID = packet.MessageID

	buf := new(bytes.Buffer)
	err = subAck.Write(buf)
	if err != nil {
		return errors.WithStack(err)
	}
	return c.Send(ctx, buf.Bytes())
}

func (c *Conn) onPuback(ctx context.Context, packet *packets.PubackPacket) error {
	c.server.logger.Debug(
		"[Broker] onPuback",
		zap.Uint16("MessageID", packet.MessageID),
	)
	messageID := packet.MessageID
	c.messageIDRing.FreeID(messageID)
	return nil
}
