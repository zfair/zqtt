package storage

import (
	"context"
	"io"

	"github.com/zfair/zqtt/src/config"
	"github.com/zfair/zqtt/src/internal/topic"
)

type QueryOptions struct {
	TTLUntil int64
	From     int64  // query message seq from
	Until    int64  // query message seq until
	Limit    uint64 // query limit
	Offset   uint64 // query offset
}

// MStorage interface for Message storage providers.
type MStorage interface {
	io.Closer
	// MStorage implements a config provider.
	config.Provider
	// Store message to the storage instance
	// returning message seq and error
	StoreMessage(ctx context.Context, m *topic.Message) (int64, error)
	// query message from storage
	QueryMessage(ctx context.Context, topic string, ssid topic.SSID, opts QueryOptions) ([]*topic.Message, error)
}

// SStorage interface for Subscription storage providers.
type SStorage interface {
	io.Closer
	// SStorage implements a config provider.
	config.Provider

	StoreSubscription(ctx context.Context, clientID string, t *topic.Topic) error
	DeleteSubscription(ctx context.Context, clientID string, t *topic.Topic) error
}

// RStorage interface Save Readed Seq For Ecah Client.
type RStorage interface {
	io.Closer
	// RStorage implements a config provider.
	config.Provider

	SaveReadSeq(ctx context.Context, clientID string, t *topic.Topic, messageSeq int64) error
	GetReadSeq(ctx context.Context, clientID string, t *topic.Topic) (int64, error)
}
