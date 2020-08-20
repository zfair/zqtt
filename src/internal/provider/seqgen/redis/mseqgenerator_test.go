package redis

import (
	"context"
	"testing"

	"github.com/zfair/zqtt/src/internal/topic"
	"go.uber.org/zap"
)

type mSeqGeneratorTestCase struct {
	topicName string
	expectSeq int64
}

// before run this test, you should spawn a redis process
// docker run --name some-redis -p 6379:6379 -d redis
func TestRedisMSeqGenerator(t *testing.T) {
	logger, err := zap.NewDevelopment()
	if err != nil {
		t.Fatal(err)
	}
	config := map[string]interface{}{
		"password": "",
		"addr":     "192.168.99.100:6379",
		"db":       "0",
	}
	s := NewMSeqGenerator(logger)
	err = s.Configure(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	seq, err := s.GenMessageSeq(context.Background(), &topic.Topic{})
	if err != nil {
		t.Fatal(err)
	}
	t.Log(seq)
}
