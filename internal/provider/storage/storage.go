package storage

import (
	"io"

	"github.com/zfair/zqtt/internal/topic"
)

type Storage interface {
	io.Closer

	Send(m *topic.Message) error
	Query(topic string, ssid string) ([]topic.Message, error)
}
