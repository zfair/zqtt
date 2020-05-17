package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/lib/pq"
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/zfair/zqtt/src/internal/provider/storage"
	"github.com/zfair/zqtt/src/internal/topic"
)

const maxTTL = 30 * 24 * time.Hour

// we build postgres index on topic parts
const maxTopicParts = 8

var _ storage.MStorage = (*MStorage)(nil)

var validConfigKeywords = []string{
	"dbname",
	"user",
	"password",
	"host",
	"port",
	"sslmode",
	"connect_timeout",
}

type MStorage struct {
	logger *zap.Logger
	db     *sql.DB
}

// NewStorage creates a new PostgresQL storage provider.
func NewStorage(logger *zap.Logger) *MStorage {
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
	var sb strings.Builder

	for _, key := range validConfigKeywords {
		if value, ok := config[key]; ok {
			cfgPart := fmt.Sprintf("%s=%s ", key, value.(string))
			if _, err := sb.WriteString(cfgPart); err != nil {
				return err
			}
		}
	}

	connStr := sb.String()
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return err
	}
	err = db.Ping()
	if err != nil {
		return err
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
func (s *MStorage) StoreMessage(ctx context.Context, m *topic.Message) (int64, error) {
	if len(m.Ssid) > maxTopicParts {
		return 0, errors.Errorf("max valid topic parts of postgres storage is %d, but got %d", maxTopicParts, len(m.Ssid))
	}

	conn, err := s.db.Conn(ctx)
	if err != nil {
		return 0, err
	}
	ssidStringArray := make(pq.StringArray, len(m.Ssid))
	for i := range m.Ssid {
		ssidStringArray[i] = strconv.FormatUint(m.Ssid[i], 10)
	}
	result := conn.QueryRowContext(
		ctx,
		`INSERT INTO message(
			guid,
			client_id,
			topic,
			ssid,
			ssid_len,
			ttl_until,
			qos,
			payload
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING message_seq`,
		m.GUID, m.ClientID, m.TopicName, ssidStringArray, len(m.Ssid), m.TTLUntil, m.Qos, string(m.Payload),
	)
	if err != nil {
		return 0, err
	}
	var messageSeq time.Time
	err = result.Scan(&messageSeq)
	if err != nil {
		return 0, err
	}
	s.logger.Info(
		"Postgres Storage Store",
		zap.String("guid", m.GUID),
		zap.String("clientID", m.ClientID),
		zap.Int64("messageSeq", messageSeq.UnixNano()),
	)
	// using message create time as message seq
	return messageSeq.UnixNano(), nil
}

// Query a topic message.
func (s *MStorage) QueryMessage(ctx context.Context, topicName string, _ssid topic.SSID, opts storage.QueryOptions) ([]*topic.Message, error) {
	// Generate Query SQL
	sql, args, err := s.queryParse(topicName, opts)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]*topic.Message, 0)
	for rows.Next() {
		mm := messageModel{}
		if err := rows.Scan(
			&mm.MessageSeq,
			&mm.GUID,
			&mm.ClientID,
			&mm.TopicName,
			&mm.Qos,
			&mm.Payload,
		); err != nil {
			return nil, err
		}
		message := topic.NewMessage(
			mm.GUID,
			mm.ClientID,
			mm.TopicName,
			nil,
			byte(mm.Qos),
			mm.TTLUntil,
			[]byte(mm.Payload),
		)
		message.SetMessageSeq(mm.MessageSeq.UnixNano())
		result = append(result, message)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *MStorage) queryParse(topicName string, opts storage.QueryOptions) (string, []interface{}, error) {
	pgSQL := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
	sqlBuilder := pgSQL.Select("message_seq, guid, client_id, topic, qos, payload").From("message")
	var zeroTime time.Time
	if opts.TTLUntil != 0 {
		sqlBuilder = sqlBuilder.Where("ttl_until <= ?", opts.TTLUntil)
	}
	if opts.From != zeroTime {
		sqlBuilder = sqlBuilder.Where("created_at >= ?", opts.From)
	}
	if opts.Until != zeroTime {
		sqlBuilder = sqlBuilder.Where("created_at < ?", opts.Until)
	}

	parts := strings.Split(topicName, "/")
	// parse topic into query string
	querySsidLen := 0
	includeMultiWildcard := false
	for i, part := range parts {
		switch part {
		case topic.MultiWildcard:
			// if match a MultiWildcard part, break
			// # must last part of topic name
			includeMultiWildcard = true
			break
		case topic.SingleWildcard:
			// just increase but do not set this part condition
			querySsidLen++
		default:
			querySsidLen++
			hashOfPart := topic.Sum64([]byte(part))
			sqlBuilder = sqlBuilder.Where(fmt.Sprintf("ssid[%d] = ?", i+1), strconv.FormatUint(hashOfPart, 10))

		}
	}

	if querySsidLen > 0 {
		if includeMultiWildcard {
			sqlBuilder = sqlBuilder.Where("ssid_len > ?", querySsidLen)
		} else {
			sqlBuilder = sqlBuilder.Where("ssid_len = ?", querySsidLen)
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
