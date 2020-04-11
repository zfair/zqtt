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

// Storage interface for storage providers.
type Storage interface {
	io.Closer
	// Storage implements a config provider.
	config.Provider
	// Store message to the storage instance.
	Store(ctx context.Context, m *topic.Message) error

	// query message from storage
	Query(ctx context.Context, topic string, ssid topic.SSID, opts QueryOptions) ([]*topic.Message, error)
}
