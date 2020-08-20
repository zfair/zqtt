package redis

import (
	"context"
	"strconv"
	"strings"

	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	"github.com/zfair/zqtt/src/internal/provider/seqgen"
	"github.com/zfair/zqtt/src/internal/topic"
	"go.uber.org/zap"
)

var _ seqgen.MSeqGenerator = (*MSeqGenerator)(nil)

type MSeqGenerator struct {
	logger *zap.Logger
	rdb    *redis.Client
}

func NewMSeqGenerator(logger *zap.Logger) *MSeqGenerator {
	return &MSeqGenerator{
		logger: logger,
	}
}

func (s *MSeqGenerator) Name() string {
	return "redis"
}

func (s *MSeqGenerator) Configure(ctx context.Context, config map[string]interface{}) error {
	addr := config["addr"].(string)
	password := config["password"].(string)
	dbstr := config["db"].(string)

	db, err := strconv.ParseInt(dbstr, 10, 32)
	if err != nil {
		return errors.WithStack(err)
	}

	opts := redis.Options{
		Addr:     addr,
		Password: password,
		DB:       int(db),
	}
	s.rdb = redis.NewClient(&opts)
	err = s.rdb.Ping(ctx).Err()
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// Close the redis connection.
func (s *MSeqGenerator) Close() error {
	return s.rdb.Close()
}

func (s *MSeqGenerator) GenMessageSeq(ctx context.Context, t *topic.Topic) (int64, error) {
	ssid := t.ToSSID()
	var parts []string
	for _, part := range ssid {
		parts = append(parts, strconv.FormatUint(part, 10))
	}
	key := strings.Join(parts, "/")
	s.logger.Info(
		"Redis Message Seq Geneator",
		zap.String("key", key),
	)
	intCMD := s.rdb.Incr(ctx, key)
	err := intCMD.Err()
	if err != nil {
		return 0, err
	}
	return intCMD.Val(), nil
}
