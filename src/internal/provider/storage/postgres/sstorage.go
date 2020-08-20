package postgres

import (
	"context"
	"database/sql"
	"strconv"

	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/zfair/zqtt/src/internal/provider/storage"
	"github.com/zfair/zqtt/src/internal/topic"
	"go.uber.org/zap"
)

var _ storage.SStorage = (*SStorage)(nil)

type SStorage struct {
	logger *zap.Logger
	db     *sql.DB
}

// NewMStorage creates a new PostgresQL subscription storage provider.
func NewSStorage(logger *zap.Logger) *SStorage {
	return &SStorage{
		logger: logger,
	}
}

// Name of PostgresQL subscription storage provider.
func (*SStorage) Name() string {
	return "postgres"
}

// Configure and connect to the storage.
func (s *SStorage) Configure(ctx context.Context, config map[string]interface{}) error {
	connStr, err := generateConnString(ctx, config)
	if err != nil {
		return errors.WithStack(err)
	}

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return errors.WithStack(err)
	}

	err = db.Ping()
	if err != nil {
		return errors.WithStack(err)
	}

	s.logger.Info("[Postgres Subscription Storage]Connected To Postgres")
	// TODO: SetMaxIdleConn and SetMaxOpenConn
	s.db = db
	return nil
}

// Close the storage connection.
func (s *SStorage) Close() error {
	return s.db.Close()
}

func (s *SStorage) StoreSubscription(ctx context.Context, username string, t *topic.Topic) error {
	ssid := t.ToSSID()
	if len(ssid) > maxTopicParts {
		return errors.Errorf("max valid topic parts of postgres storage is %d, but got %d", maxTopicParts, len(ssid))
	}

	ssidStringArray := make(pq.StringArray, len(ssid))
	for i := range ssid {
		ssidStringArray[i] = strconv.FormatUint(ssid[i], 10)
	}

	conn, err := s.db.Conn(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	defer conn.Close()

	_, err = conn.ExecContext(
		ctx,
		`INSERT INTO subscription(
			username,
			topic,
			ssid,
			ssid_len
		) VALUES ($1, $2, $3, $4)`,
		username, t.TopicName(), ssidStringArray, len(ssid),
	)
	if err != nil {
		return errors.WithStack(err)
	}

	s.logger.Info(
		"Postgres Subscription Storage StoreSubscription",
		zap.String("username", username),
		zap.String("topicName", t.TopicName()),
	)

	return nil
}

func (s *SStorage) DeleteSubscription(ctx context.Context, username string, t *topic.Topic) error {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	defer conn.Close()

	_, err = conn.ExecContext(
		ctx,
		`DELETE FROM subscription WHERE username = $1 AND topic = $2`,
		username, t.TopicName(),
	)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}
