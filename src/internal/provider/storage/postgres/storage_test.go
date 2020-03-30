package postgres

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

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

type postgresStorageTestCase struct {
	queryTopicName string
	queryOptions   storage.QueryOptions
	err            error
	matchCount     int
	matchTopicsID  map[string]bool
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
		"connect_timeout": "10",
	}
	store := NewStorage(logger)
	err = store.Configure(config)
	if err != nil {
		t.Fatal(err)
	}

	messageTopicNames := []string{
		"foo",
		"hello",
		"foo/bar",
		"hello/world",
		"hello/mqtt",
		"hello/foo/bar",
		"hello/world/foo/bar",
		"hello/world/zqtt",
		"hello/mqtt/zqtt",
		"hello/mqtt/zqtt/foo",
		"hello/mqtt/zqtt/bar",
		"hello/mqtt/zqtt/foo/bar",
	}
	var messages []*topic.Message
	for i, name := range messageTopicNames {
		m := topic.NewMessage(
			name,            // message topic name as message guid
			strconv.Itoa(i), // element index of messages as clientID
			uint16(i),
			name,
			parseTopic(name),
			0,
			0,
			[]byte(name),
		)
		messages = append(messages, m)
	}
	for _, message := range messages {
		err = store.Store(context.Background(), message)
		if err != nil {
			t.Fatal(err)
		}
	}
	// TODO(zerolocusta) Add More Query Test
	testCases := []postgresStorageTestCase{
		{
			queryTopicName: "#",
			matchCount:     len(messageTopicNames),
			matchTopicsID: map[string]bool{
				"foo":                     true,
				"hello":                   true,
				"foo/bar":                 true,
				"hello/world":             true,
				"hello/mqtt":              true,
				"hello/foo/bar":           true,
				"hello/world/foo/bar":     true,
				"hello/world/zqtt":        true,
				"hello/mqtt/zqtt":         true,
				"hello/mqtt/zqtt/foo":     true,
				"hello/mqtt/zqtt/bar":     true,
				"hello/mqtt/zqtt/foo/bar": true,
			},
		},
	}

	for _, testCase := range testCases {
		result, err := store.Query(context.Background(), testCase.queryTopicName, nil, testCase.queryOptions)
		if err != nil {
			t.Fatal(err)
		}
		// we use message topic name as it's GUID
		resultGUIDSet := make(map[string]bool)
		for _, ele := range result {
			resultGUIDSet[ele.GUID] = true
		}
		assertion := assert.New(t)
		assertion.Equal(len(resultGUIDSet), len(result))
		assertion.Equal(len(resultGUIDSet), testCase.matchCount)
		assertion.Equal(len(resultGUIDSet), len(testCase.matchTopicsID))
		for k := range testCase.matchTopicsID {
			_, ok := resultGUIDSet[k]
			if !ok {
				assertion.Equal(true, ok)
			}
		}
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
			SQL:       "SELECT message_seq, guid, client_id, message_id, topic, qos, payload FROM message",
		},
		{
			TopicName: "hello/#",
			SQL:       "SELECT message_seq, guid, client_id, message_id, topic, qos, payload FROM message WHERE ssid[0] = $1 AND ssid_len > $2",
			Args:      []interface{}{topic.Sum64([]byte("hello")), 1},
		},
		{
			TopicName: "hello/+/+",
			SQL:       "SELECT message_seq, guid, client_id, message_id, topic, qos, payload FROM message WHERE ssid[0] = $1 AND ssid_len = $2",
			Args:      []interface{}{topic.Sum64([]byte("hello")), 3},
		},
		{
			TopicName: "hello/+/world",
			SQL:       "SELECT message_seq, guid, client_id, message_id, topic, qos, payload FROM message WHERE ssid[0] = $1 AND ssid[2] = $2 AND ssid_len = $3",
			Args:      []interface{}{topic.Sum64([]byte("hello")), topic.Sum64([]byte("world")), 3},
		},
		{
			TopicName: "hello/+/world/+",
			SQL:       "SELECT message_seq, guid, client_id, message_id, topic, qos, payload FROM message WHERE ssid[0] = $1 AND ssid[2] = $2 AND ssid_len = $3",
			Args:      []interface{}{topic.Sum64([]byte("hello")), topic.Sum64([]byte("world")), 4},
		},
		{
			TopicName: "hello/+/world",
			Options: storage.QueryOptions{
				TTLUntil: 1919,
			},
			SQL:  "SELECT message_seq, guid, client_id, message_id, topic, qos, payload FROM message WHERE ttl_until <= $1 AND ssid[0] = $2 AND ssid[2] = $3 AND ssid_len = $4",
			Args: []interface{}{int64(1919), topic.Sum64([]byte("hello")), topic.Sum64([]byte("world")), 3},
		},
		{
			TopicName: "hello/+/world",
			Options: storage.QueryOptions{
				TTLUntil: 1919,
				From:     fromTime,
			},
			SQL:  "SELECT message_seq, guid, client_id, message_id, topic, qos, payload FROM message WHERE ttl_until <= $1 AND created_at >= $2 AND ssid[0] = $3 AND ssid[2] = $4 AND ssid_len = $5",
			Args: []interface{}{int64(1919), fromTime, topic.Sum64([]byte("hello")), topic.Sum64([]byte("world")), 3},
		},
		{
			TopicName: "hello/+/world",
			Options: storage.QueryOptions{
				TTLUntil: 1919,
				From:     fromTime,
				Until:    untilTime,
			},
			SQL:  "SELECT message_seq, guid, client_id, message_id, topic, qos, payload FROM message WHERE ttl_until <= $1 AND created_at >= $2 AND created_at < $3 AND ssid[0] = $4 AND ssid[2] = $5 AND ssid_len = $6",
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
			SQL:  "SELECT message_seq, guid, client_id, message_id, topic, qos, payload FROM message WHERE ttl_until <= $1 AND created_at >= $2 AND created_at < $3 AND ssid[0] = $4 AND ssid[2] = $5 AND ssid_len = $6 LIMIT 10",
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
			SQL:  "SELECT message_seq, guid, client_id, message_id, topic, qos, payload FROM message WHERE ttl_until <= $1 AND created_at >= $2 AND created_at < $3 AND ssid[0] = $4 AND ssid[2] = $5 AND ssid_len = $6 LIMIT 10 OFFSET 100",
			Args: []interface{}{int64(1919), fromTime, untilTime, topic.Sum64([]byte("hello")), topic.Sum64([]byte("world")), 3},
		},
	}
	logger, err := zap.NewDevelopment()
	if err != nil {
		t.Fatal(err)
	}
	store := NewStorage(logger)
	for _, c := range testCase {
		sql, args, err := store.queryParse(c.TopicName, c.Options)
		if err != nil {
			t.Fatal(err)
		}
		assertion := assert.New(t)
		assertion.Equal(c.SQL, sql)
		assertion.Equal(c.Args, args)
	}
}
