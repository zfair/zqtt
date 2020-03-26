package postgres

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/zfair/zqtt/src/internal/provider/storage"
	"github.com/zfair/zqtt/src/internal/topic"
)

func parseTopic(topicName string) []uint64 {
	parts := strings.Split(topicName, "/")
	ssid := make([]uint64, len(parts))
	for i, part := range parts {
		v := topic.Sum64([]byte(part))
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
	ttl := int64(1)
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

type queryParseTestCase struct {
	TopicName string
	Options   storage.QueryOptions
	SQL       string
	Args      []interface{}
	Err       error
}

func TestStorageQueryParse(t *testing.T) {
	fromTime := time.Now()
	untilTime := time.Now()
	testCase := []queryParseTestCase{
		{
			TopicName: "#",
			SQL:       "SELECT message_seq, guid, client_id, topic, qos, payload FROM message",
		},
		{
			TopicName: "hello/#",
			SQL:       "SELECT message_seq, guid, client_id, topic, qos, payload FROM message WHERE ssid[0] = $1 AND ssid_len > $2",
			Args:      []interface{}{topic.Sum64([]byte("hello")), 1},
		},
		{
			TopicName: "hello/+/+",
			SQL:       "SELECT message_seq, guid, client_id, topic, qos, payload FROM message WHERE ssid[0] = $1 AND ssid_len = $2",
			Args:      []interface{}{topic.Sum64([]byte("hello")), 3},
		},
		{
			TopicName: "hello/+/world",
			SQL:       "SELECT message_seq, guid, client_id, topic, qos, payload FROM message WHERE ssid[0] = $1 AND ssid[2] = $2 AND ssid_len = $3",
			Args:      []interface{}{topic.Sum64([]byte("hello")), topic.Sum64([]byte("world")), 3},
		},
		{
			TopicName: "hello/+/world/+",
			SQL:       "SELECT message_seq, guid, client_id, topic, qos, payload FROM message WHERE ssid[0] = $1 AND ssid[2] = $2 AND ssid_len = $3",
			Args:      []interface{}{topic.Sum64([]byte("hello")), topic.Sum64([]byte("world")), 4},
		},
		{
			TopicName: "hello/+/world",
			Options: storage.QueryOptions{
				TTLUntil: 1919,
			},
			SQL:  "SELECT message_seq, guid, client_id, topic, qos, payload FROM message WHERE ttl_until <= $1 AND ssid[0] = $2 AND ssid[2] = $3 AND ssid_len = $4",
			Args: []interface{}{int64(1919), topic.Sum64([]byte("hello")), topic.Sum64([]byte("world")), 3},
		},
		{
			TopicName: "hello/+/world",
			Options: storage.QueryOptions{
				TTLUntil: 1919,
				From:     fromTime,
			},
			SQL:  "SELECT message_seq, guid, client_id, topic, qos, payload FROM message WHERE ttl_until <= $1 AND created_at >= $2 AND ssid[0] = $3 AND ssid[2] = $4 AND ssid_len = $5",
			Args: []interface{}{int64(1919), fromTime, topic.Sum64([]byte("hello")), topic.Sum64([]byte("world")), 3},
		},
		{
			TopicName: "hello/+/world",
			Options: storage.QueryOptions{
				TTLUntil: 1919,
				From:     fromTime,
				Until:    untilTime,
			},
			SQL:  "SELECT message_seq, guid, client_id, topic, qos, payload FROM message WHERE ttl_until <= $1 AND created_at >= $2 AND created_at < $3 AND ssid[0] = $4 AND ssid[2] = $5 AND ssid_len = $6",
			Args: []interface{}{int64(1919), fromTime, untilTime, topic.Sum64([]byte("hello")), topic.Sum64([]byte("world")), 3},
		},
		{
			TopicName: "hello/+/world",
			Options: storage.QueryOptions{
				TTLUntil: 1919,
				From:     fromTime,
				Until:    untilTime,
				Limit:    10,
			},
			SQL:  "SELECT message_seq, guid, client_id, topic, qos, payload FROM message WHERE ttl_until <= $1 AND created_at >= $2 AND created_at < $3 AND ssid[0] = $4 AND ssid[2] = $5 AND ssid_len = $6 LIMIT 10",
			Args: []interface{}{int64(1919), fromTime, untilTime, topic.Sum64([]byte("hello")), topic.Sum64([]byte("world")), 3},
		},
		{
			TopicName: "hello/+/world",
			Options: storage.QueryOptions{
				TTLUntil: 1919,
				From:     fromTime,
				Until:    untilTime,
				Limit:    10,
				Offset:   100,
			},
			SQL:  "SELECT message_seq, guid, client_id, topic, qos, payload FROM message WHERE ttl_until <= $1 AND created_at >= $2 AND created_at < $3 AND ssid[0] = $4 AND ssid[2] = $5 AND ssid_len = $6 LIMIT 10 OFFSET 100",
			Args: []interface{}{int64(1919), fromTime, untilTime, topic.Sum64([]byte("hello")), topic.Sum64([]byte("world")), 3},
		},
	}
	logger, err := zap.NewDevelopment()
	if err != nil {
		t.Fatal(err)
	}
	storage := NewStorage(logger)
	for _, c := range testCase {
		sql, args, err := storage.queryParse(c.TopicName, c.Options)
		if err != nil {
			t.Fatal(err)
		}
		assert := assert.New(t)
		assert.Equal(c.SQL, sql)
		assert.Equal(c.Args, args)

	}
}
