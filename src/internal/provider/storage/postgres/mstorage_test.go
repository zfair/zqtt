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
	"github.com/zfair/zqtt/src/zqttpb"
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

func Sum64String(b []byte) string {
	return strconv.FormatUint(topic.Sum64(b), 10)
}

type postgresStorageTestCase struct {
	queryTopicName string
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
		"host":            "192.168.99.100",
		"port":            "5432",
		"sslmode":         "disable",
		"connect_timeout": "10",
	}
	store := NewMStorage(logger)
	err = store.Configure(context.Background(), config)
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
	var messages []*zqttpb.Message
	for i, name := range messageTopicNames {
		m := &zqttpb.Message{
			MessageSeq: int64(i),
			Username:   "test",
			ClientID:   strconv.FormatInt(int64(i), 10),
			TopicName:  name,
			Ssid:       parseTopic(name),
			Qos:        0,
			Type:       zqttpb.MsgText,
			Payload:    []byte(name),
			CreatedAt:  time.Now().UnixNano(),
		}
		messages = append(messages, m)
	}
	for _, message := range messages {
		err := store.StoreMessage(context.Background(), message)
		if err != nil {
			t.Fatal(err)
		}
	}
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
		{
			queryTopicName: "+",
			matchCount:     2,
			matchTopicsID: map[string]bool{
				"foo":   true,
				"hello": true,
			},
		},
		{
			queryTopicName: "hello/#",
			matchCount:     9,
			matchTopicsID: map[string]bool{
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
		{
			queryTopicName: "hello/+",
			matchCount:     2,
			matchTopicsID: map[string]bool{
				"hello/world": true,
				"hello/mqtt":  true,
			},
		},
		{
			queryTopicName: "hello/+/zqtt",
			matchCount:     2,
			matchTopicsID: map[string]bool{
				"hello/world/zqtt": true,
				"hello/mqtt/zqtt":  true,
			},
		},
		{
			queryTopicName: "hello/mqtt/#",
			matchCount:     4,
			matchTopicsID: map[string]bool{
				"hello/mqtt/zqtt":         true,
				"hello/mqtt/zqtt/foo":     true,
				"hello/mqtt/zqtt/bar":     true,
				"hello/mqtt/zqtt/foo/bar": true,
			},
		},
		{
			queryTopicName: "hello/mqtt/+",
			matchCount:     1,
			matchTopicsID: map[string]bool{
				"hello/mqtt/zqtt": true,
			},
		},
		{
			queryTopicName: "hello/mqtt/zqtt",
			matchCount:     1,
			matchTopicsID: map[string]bool{
				"hello/mqtt/zqtt": true,
			},
		},
		{
			queryTopicName: "hello/mqtt/+/+",
			matchCount:     2,
			matchTopicsID: map[string]bool{
				"hello/mqtt/zqtt/foo": true,
				"hello/mqtt/zqtt/bar": true,
			},
		},
		{
			queryTopicName: "hello/mqtt/+/foo",
			matchCount:     1,
			matchTopicsID: map[string]bool{
				"hello/mqtt/zqtt/foo": true,
			},
		},
		{
			queryTopicName: "hello/mqtt/zqtt/foo",
			matchCount:     1,
			matchTopicsID: map[string]bool{
				"hello/mqtt/zqtt/foo": true,
			},
		},
	}

	for _, testCase := range testCases {
		tp, err := topic.NewParser(testCase.queryTopicName).Parse()
		if err != nil {
			t.Fatal(err)
		}
		opts := storage.QueryOptions{
			Topic: tp,
		}
		result, err := store.QueryMessage(context.Background(), opts)
		if err != nil {
			t.Fatal(err)
		}
		resultSet := make(map[string]bool)
		for _, ele := range result {
			resultSet[ele.TopicName+strconv.FormatInt(ele.MessageSeq, 10)] = true
		}
		assertion := assert.New(t)
		assertion.Equal(len(resultSet), len(result))
		assertion.Equal(testCase.matchCount, testCase.matchCount)
		assertion.Equal(len(testCase.matchTopicsID), len(resultSet))
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
	fromSeq := time.Now().UnixNano()
	untilSeq := time.Now().UnixNano()

	testCase := []queryParseTestCase{
		{
			TopicName: "#",
			SQL:       "SELECT message_seq, username, client_id, topic, qos, type, payload, created_at FROM message",
		},
		{
			TopicName: "hello/#",
			SQL:       "SELECT message_seq, username, client_id, topic, qos, type, payload, created_at FROM message WHERE ssid[1] = $1 AND ssid_len > $2",
			Args:      []interface{}{Sum64String([]byte("hello")), 1},
		},
		{
			TopicName: "hello/+/+",
			SQL:       "SELECT message_seq, username, client_id, topic, qos, type, payload, created_at FROM message WHERE ssid[1] = $1 AND ssid_len = $2",
			Args:      []interface{}{Sum64String([]byte("hello")), 3},
		},
		{
			TopicName: "hello/+/world",
			SQL:       "SELECT message_seq, username, client_id, topic, qos, type, payload, created_at FROM message WHERE ssid[1] = $1 AND ssid[3] = $2 AND ssid_len = $3",
			Args:      []interface{}{Sum64String([]byte("hello")), Sum64String([]byte("world")), 3},
		},
		{
			TopicName: "hello/+/world/+",
			SQL:       "SELECT message_seq, username, client_id, topic, qos, type, payload, created_at FROM message WHERE ssid[1] = $1 AND ssid[3] = $2 AND ssid_len = $3",
			Args:      []interface{}{Sum64String([]byte("hello")), Sum64String([]byte("world")), 4},
		},
		{
			TopicName: "hello/+/world",
			SQL:       "SELECT message_seq, username, client_id, topic, qos, type, payload, created_at FROM message WHERE ssid[1] = $1 AND ssid[3] = $2 AND ssid_len = $3",
			Args:      []interface{}{Sum64String([]byte("hello")), Sum64String([]byte("world")), 3},
		},
		{
			TopicName: "hello/+/world",
			Options: storage.QueryOptions{
				FromSeq: fromSeq,
			},
			SQL:  "SELECT message_seq, username, client_id, topic, qos, type, payload, created_at FROM message WHERE message_seq >= $1 AND ssid[1] = $2 AND ssid[3] = $3 AND ssid_len = $4",
			Args: []interface{}{fromSeq, Sum64String([]byte("hello")), Sum64String([]byte("world")), 3},
		},
		{
			TopicName: "hello/+/world",
			Options: storage.QueryOptions{
				FromSeq:  fromSeq,
				UntilSeq: untilSeq,
			},
			SQL:  "SELECT message_seq, username, client_id, topic, qos, type, payload, created_at FROM message WHERE message_seq >= $1 AND message_seq < $2 AND ssid[1] = $3 AND ssid[3] = $4 AND ssid_len = $5",
			Args: []interface{}{fromSeq, untilSeq, Sum64String([]byte("hello")), Sum64String([]byte("world")), 3},
		},
		{
			TopicName: "hello/+/world",
			Options: storage.QueryOptions{
				FromSeq:  fromSeq,
				UntilSeq: untilSeq,
				Limit:    10,
			},
			SQL:  "SELECT message_seq, username, client_id, topic, qos, type, payload, created_at FROM message WHERE message_seq >= $1 AND message_seq < $2 AND ssid[1] = $3 AND ssid[3] = $4 AND ssid_len = $5 LIMIT 10",
			Args: []interface{}{fromSeq, untilSeq, Sum64String([]byte("hello")), Sum64String([]byte("world")), 3},
		},
		{
			TopicName: "hello/+/world",
			Options: storage.QueryOptions{
				FromSeq:  fromSeq,
				UntilSeq: untilSeq,
				Limit:    10,
				Offset:   100,
			},
			SQL:  "SELECT message_seq, username, client_id, topic, qos, type, payload, created_at FROM message WHERE message_seq >= $1 AND message_seq < $2 AND ssid[1] = $3 AND ssid[3] = $4 AND ssid_len = $5 LIMIT 10 OFFSET 100",
			Args: []interface{}{fromSeq, untilSeq, Sum64String([]byte("hello")), Sum64String([]byte("world")), 3},
		},
		{
			TopicName: "hello/+/world",
			Options: storage.QueryOptions{
				FromSeq:  fromSeq,
				UntilSeq: untilSeq,
				FromTime: 1596641026797989000,
				Limit:    10,
				Offset:   100,
			},
			SQL:  "SELECT message_seq, username, client_id, topic, qos, type, payload, created_at FROM message WHERE message_seq >= $1 AND message_seq < $2 AND created_at >= $3 AND ssid[1] = $4 AND ssid[3] = $5 AND ssid_len = $6 LIMIT 10 OFFSET 100",
			Args: []interface{}{fromSeq, untilSeq, time.Unix(0, 1596641026797989000), Sum64String([]byte("hello")), Sum64String([]byte("world")), 3},
		},
		{
			TopicName: "hello/+/world",
			Options: storage.QueryOptions{
				FromSeq:   fromSeq,
				UntilSeq:  untilSeq,
				FromTime:  1596641026797989000,
				UntilTime: 1696641026797989000,
				Limit:     10,
				Offset:    100,
			},
			SQL:  "SELECT message_seq, username, client_id, topic, qos, type, payload, created_at FROM message WHERE message_seq >= $1 AND message_seq < $2 AND created_at >= $3 AND created_at < $4 AND ssid[1] = $5 AND ssid[3] = $6 AND ssid_len = $7 LIMIT 10 OFFSET 100",
			Args: []interface{}{fromSeq, untilSeq, time.Unix(0, 1596641026797989000), time.Unix(0, 1696641026797989000), Sum64String([]byte("hello")), Sum64String([]byte("world")), 3},
		},
	}
	logger, err := zap.NewDevelopment()
	if err != nil {
		t.Fatal(err)
	}
	store := NewMStorage(logger)
	for _, c := range testCase {
		tp, err := topic.NewParser(c.TopicName).Parse()
		if err != nil {
			t.Fatal(err)
		}
		c.Options.Topic = tp
		sql, args, err := store.queryParse(c.Options)
		if err != nil {
			t.Fatal(err)
		}
		assertion := assert.New(t)
		assertion.Equal(c.SQL, sql)
		assertion.Equal(c.Args, args)
	}
}
