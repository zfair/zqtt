package postgres

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/spaolacci/murmur3"
	"go.uber.org/zap"

	"github.com/zfair/zqtt/internal/topic"
)

func parseTopic(topic string) []uint64 {
	parts := strings.Split(topic, "/")
	ssid := make([]uint64, len(parts))
	for i, part := range parts {
		v := murmur3.Sum64([]byte(part))
		ssid[i] = v
	}
	return ssid
}

// before run this test, you should spawn a postgres process
// docker run -d --name some-postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres
func TestPostgresStorage(t *testing.T) {
	logger, err := zap.NewDevelopment()
	if err != nil {
		t.Fatal(err)
	}
	config := map[string]interface{}{
		"dbname":          "postgres",
		"user":            "postgres",
		"password":        "postgres",
		"host":            "127.0.0.1",
		"port":            "5432",
		"sslmode":         "disable",
		"connect_timeout": float64(10),
	}
	storage := NewStorage(logger)
	err = storage.Configure(config)
	if err != nil {
		t.Fatal(err)
	}
	uid, err := uuid.NewRandom()
	if err != nil {
		t.Fatal(err)
	}
	guid := uid.String()
	clientID := "test"
	messageID := uint16(0)
	topicName := "foo/bar"
	ssid := parseTopic(topicName)
	qos := byte(0)
	ttl := 1
	payload := []byte("hello world!")
	message := topic.NewMessage(
		guid,
		clientID,
		messageID,
		topicName,
		ssid,
		qos,
		ttl,
		payload,
	)
	err = storage.Store(context.Background(), message)
	if err != nil {
		t.Fatal(err)
	}
}
