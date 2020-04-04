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
	"go.uber.org/zap"

	"github.com/zfair/zqtt/src/internal/provider/storage"
	"github.com/zfair/zqtt/src/internal/topic"
)

const maxTTL = 30 * 24 * time.Hour

var _ storage.Storage = (*Storage)(nil)

var validConfigKeywords = []string{
	"dbname",
	"user",
	"password",
	"host",
	"port",
	"sslmode",
	"connect_timeout",
}

type Storage struct {
	logger *zap.Logger
	db     *sql.DB
}

// NewStorage creates a new PostgresQL storage provider.
func NewStorage(logger *zap.Logger) *Storage {
	return &Storage{
		logger: logger,
	}
}

// Name of PostgresQL storage provider.
func (*Storage) Name() string {
	return "postgres"
}

// Configure and connect to the storage.
func (s *Storage) Configure(ctx context.Context, config map[string]interface{}) error {
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
	// TODO: SetMaxIdleConn and SetMaxOpenConn
	s.db = db
	return nil
}

// Close the storage connection.
func (s *Storage) Close() error {
	return s.db.Close()
}

// Store a topic message.
func (s *Storage) Store(ctx context.Context, m *topic.Message) error {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	ssidStringArray := make(pq.StringArray, len(m.Ssid))
	for i := range m.Ssid {
		ssidStringArray[i] = strconv.FormatUint(m.Ssid[i], 10)
	}
	_, err = conn.ExecContext(
		ctx,
		`INSERT INTO message(
			guid,
			client_id,
			message_id,
			topic,
			ssid,
			ssid_len,
			ttl_until,
			qos,
			payload
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		m.GUID, m.ClientID, m.MessageID, m.TopicName, ssidStringArray, len(m.Ssid), m.TTLUntil, m.Qos, string(m.Payload),
	)
	if err != nil {
		return err
	}
	s.logger.Info(
		"Postgres Storage Store",
		zap.String("guid", m.GUID),
		zap.String("clientID", m.ClientID),
	)
	return nil
}

// Query a topic message.
func (s *Storage) Query(ctx context.Context, topicName string, _ssid topic.SSID, opts storage.QueryOptions) ([]*topic.Message, error) {
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
			&mm.MessageID,
			&mm.TopicName,
			&mm.Qos,
			&mm.Payload,
		); err != nil {
			return nil, err
		}
		message := topic.NewMessage(
			mm.GUID,
			mm.ClientID,
			uint16(mm.MessageID),
			mm.TopicName,
			nil,
			byte(mm.Qos),
			mm.TTLUntil,
			[]byte(mm.Payload),
		)
		result = append(result, message)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Storage) queryParse(topicName string, opts storage.QueryOptions) (string, []interface{}, error) {
	pgSQL := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
	sqlBuilder := pgSQL.Select("message_seq, guid, client_id, message_id, topic, qos, payload").From("message")
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
