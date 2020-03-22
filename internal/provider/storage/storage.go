package storage

import (
	"io"
	"time"

	"github.com/zfair/zqtt/internal/config"

	"github.com/zfair/zqtt/internal/topic"
)

type QueryOptions struct {
	from   time.Time // query message save time from unix seconds
	until  time.Time // query message save time until unix seconds
	limit  int       // query limit
	offset int       // query offset
}

type Storage interface {
	io.Closer
	// Storage implements a config provider
	config.Provider
	// Store message to the storage instance
	Store(m *topic.Message) error

	// query message from storage
	Query(topic string, ssid string, opts QueryOptions) ([]topic.Message, error)
}
