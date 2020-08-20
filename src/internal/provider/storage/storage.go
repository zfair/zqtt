package storage

import (
	"context"
	"io"

	"github.com/zfair/zqtt/src/config"
	"github.com/zfair/zqtt/src/internal/topic"
	"github.com/zfair/zqtt/src/zqttpb"
)

type QueryOptions struct {
	Type      zqttpb.MessageType
	Topic     *topic.Topic
	Username  string
	FromSeq   int64  // query message seq from
	UntilSeq  int64  // query message seq until
	FromTime  int64  // query message create time from (unixnano)
	UntilTime int64  // query message create time until (unixnano)
	Limit     uint64 // query limit
	Offset    uint64 // query offset
}

// MStorage interface for Message storage providers.
type MStorage interface {
	io.Closer
	// MStorage implements a config provider.
	config.Provider
	// Store message to the storage instance
	// returning message seq and error
	StoreMessage(ctx context.Context, m *zqttpb.Message) error
	// query message from storage
	QueryMessage(ctx context.Context, opts QueryOptions) ([]*zqttpb.Message, error)
}

// SStorage interface for Subscription storage providers.
type SStorage interface {
	io.Closer
	// SStorage implements a config provider.
	config.Provider

	StoreSubscription(ctx context.Context, username string, t *topic.Topic) error
	DeleteSubscription(ctx context.Context, username string, t *topic.Topic) error
}

type MessageAckRecord struct {
	TopicName  string
	MessageSeq int64
}

// MAckStorage interface Save Message Ack For Ecah User.
type MAckStorage interface {
	io.Closer
	// RStorage implements a config provider.
	config.Provider

	// SaveReadSeq Only allow save TopicKindStatic topic
	SaveMessageAck(ctx context.Context, username string, t *topic.Topic, messageSeq int64) error

	// GetMessageAck allow Get TopicKindStatic Or TopicKindWildcard topic
	GetMessageAck(ctx context.Context, username string, t *topic.Topic) ([]MessageAckRecord, error)
}
