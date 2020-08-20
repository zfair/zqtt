package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/lib/pq"
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/zfair/zqtt/src/internal/provider/storage"
	"github.com/zfair/zqtt/src/internal/topic"
	"github.com/zfair/zqtt/src/zqttpb"
)

const maxTTL = 30 * 24 * time.Hour

var _ storage.MStorage = (*MStorage)(nil)

type MStorage struct {
	logger *zap.Logger
	db     *sql.DB
}

// NewStorage creates a new PostgresQL storage provider.
func NewMStorage(logger *zap.Logger) *MStorage {
	return &MStorage{
		logger: logger,
	}
}

// Name of PostgresQL storage provider.
func (*MStorage) Name() string {
	return "postgres"
}

// Configure and connect to the storage.
func (s *MStorage) Configure(ctx context.Context, config map[string]interface{}) error {
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
	s.logger.Info("[Postgres Message Storage]Connected To Postgres")
	// TODO: SetMaxIdleConn and SetMaxOpenConn
	s.db = db
	return nil
}

// Close the storage connection.
func (s *MStorage) Close() error {
	return s.db.Close()
}

// Store a topic message.
func (s *MStorage) StoreMessage(ctx context.Context, m *zqttpb.Message) error {
	if len(m.Ssid) > maxTopicParts {
		return errors.Errorf("max valid topic parts of postgres storage is %d, but got %d", maxTopicParts, len(m.Ssid))
	}

	ssidStringArray := make(pq.StringArray, len(m.Ssid))
	for i := range m.Ssid {
		ssidStringArray[i] = strconv.FormatUint(m.Ssid[i], 10)
	}

	conn, err := s.db.Conn(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	defer conn.Close()

	_, err = conn.ExecContext(
		ctx,
		`INSERT INTO message(
			message_seq,
			username,
			client_id,
			topic,
			ssid,
			ssid_len,
			qos,
			type,
			payload,
			created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		m.MessageSeq,
		m.Username,
		m.ClientID,
		m.TopicName,
		ssidStringArray,
		len(m.Ssid),
		m.Qos,
		m.Type,
		string(m.Payload),
		time.Unix(0, m.CreatedAt),
	)
	if err != nil {
		return errors.WithStack(err)
	}
	s.logger.Info(
		"Postgres Message Storage Store",
		zap.String("username", m.Username),
		zap.String("clientID", m.ClientID),
		zap.Int64("messageSeq", m.MessageSeq),
	)
	// using message create time as message seq
	return nil
}

// Query a topic message.
func (s *MStorage) QueryMessage(ctx context.Context, opts storage.QueryOptions) ([]*zqttpb.Message, error) {
	// Generate Query SQL
	sql, args, err := s.queryParse(opts)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]*zqttpb.Message, 0)
	for rows.Next() {
		var createdAt time.Time
		message := zqttpb.Message{}
		if err := rows.Scan(
			&message.MessageSeq,
			&message.Username,
			&message.ClientID,
			&message.TopicName,
			&message.Qos,
			&message.Type,
			&message.Payload,
			&createdAt,
		); err != nil {
			return nil, err
		}
		message.CreatedAt = createdAt.UnixNano()
		result = append(result, &message)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *MStorage) queryParse(opts storage.QueryOptions) (string, []interface{}, error) {
	pgSQL := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
	sqlBuilder := pgSQL.Select("message_seq, username, client_id, topic, qos, type, payload, created_at").From("message")
	if opts.Type != zqttpb.MsgNone {
		sqlBuilder = sqlBuilder.Where("type = ?", opts.Type)
	}
	if opts.Username != "" {
		sqlBuilder = sqlBuilder.Where("username = ?", opts.Type)
	}
	if opts.FromSeq != 0 {
		sqlBuilder = sqlBuilder.Where("message_seq >= ?", opts.FromSeq)
	}
	if opts.UntilSeq != 0 {
		sqlBuilder = sqlBuilder.Where("message_seq < ?", opts.UntilSeq)
	}
	if opts.FromTime != 0 {
		sqlBuilder = sqlBuilder.Where("created_at >= ?", time.Unix(0, opts.FromTime))
	}
	if opts.UntilTime != 0 {
		sqlBuilder = sqlBuilder.Where("created_at < ?", time.Unix(0, opts.UntilTime))
	}

	// parse topic into query string
	if opts.Topic != nil {
		ssid := opts.Topic.ToSSID()
		querySsidLen := 0
		includeMultiWildcard := false
		for i, part := range ssid {
			switch part {
			case topic.MultiWildcardHash:
				// if match a MultiWildcard part, break
				// # must last part of topic name
				includeMultiWildcard = true
				break
			case topic.SingleWildcardHash:
				// just increase but do not set this part condition
				querySsidLen++
			default:
				querySsidLen++
				sqlBuilder = sqlBuilder.Where(fmt.Sprintf("ssid[%d] = ?", i+1), strconv.FormatUint(ssid[i], 10))
			}
		}

		if querySsidLen > 0 {
			if includeMultiWildcard {
				sqlBuilder = sqlBuilder.Where("ssid_len > ?", querySsidLen)
			} else {
				sqlBuilder = sqlBuilder.Where("ssid_len = ?", querySsidLen)
			}
		}
	}

	if opts.Limit != 0 {
		sqlBuilder = sqlBuilder.Limit(opts.Limit)
	}

	if opts.Offset != 0 {
		sqlBuilder = sqlBuilder.Offset(opts.Offset)
	}

	return sqlBuilder.ToSql()
}
