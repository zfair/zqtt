package storage

import (
	"context"
	"io"
	"time"

	"github.com/zfair/zqtt/src/config"
	"github.com/zfair/zqtt/src/internal/topic"
)

type QueryOptions struct {
	TTLUntil int64
	From     time.Time // query message save time from unix seconds
	Until    time.Time // query message save time until unix seconds
	Limit    uint64    // query limit
	Offset   uint64    // query offset
}

// MStorage interface for Message storage providers.
type MStorage interface {
	io.Closer
	// Storage implements a config provider.
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
	// Storage implements a config provider.
	config.Provider

	StoreSubscription(ctx context.Context, clientID string, t *topic.Topic) error
	DeleteSubscription(ctx context.Context, clientID string, t *topic.Topic) error
}
