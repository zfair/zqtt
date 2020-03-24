package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	pq "github.com/lib/pq"
	"github.com/zfair/zqtt/internal/provider/storage"
	"github.com/zfair/zqtt/internal/topic"
	"go.uber.org/zap"
)

var _ storage.Storage = new(Storage)

type Storage struct {
	logger *zap.Logger
	db     *sql.DB
}

func NewStorage(logger *zap.Logger) *Storage {
	return &Storage{
		logger: logger,
	}
}

func (s *Storage) Name() string {
	return "postgres"
}

func (s *Storage) Configure(config map[string]interface{}) error {
	var sb strings.Builder
	dbname, ok := config["dbname"]
	if ok {
		_, err := sb.WriteString(fmt.Sprintf("dbname=%s ", dbname.(string)))
		if err != nil {
			return err
		}
	}
	username, ok := config["user"]
	if ok {
		_, err := sb.WriteString(fmt.Sprintf("user=%s ", username.(string)))
		if err != nil {
			return err
		}
	}
	password, ok := config["password"]
	if ok {
		_, err := sb.WriteString(fmt.Sprintf("password=%s ", password.(string)))
		if err != nil {
			return err
		}
	}
	host, ok := config["host"]
	if ok {
		_, err := sb.WriteString(fmt.Sprintf("host=%s ", host.(string)))
		if err != nil {
			return err
		}
	}
	port, ok := config["port"]
	if ok {
		_, err := sb.WriteString(fmt.Sprintf("port=%s ", port.(string)))
		if err != nil {
			return err
		}
	}
	sslmode, ok := config["sslmode"]
	if ok {
		_, err := sb.WriteString(fmt.Sprintf("sslmode=%s ", sslmode.(string)))
		if err != nil {
			return err
		}
	}
	connectTimeout, ok := config["connect_timeout"]
	if ok {
		_, err := sb.WriteString(fmt.Sprintf("connect_timeout=%d ", int(connectTimeout.(float64))))
		if err != nil {
			return err
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

func (s *Storage) Close() error {
	return s.db.Close()
}

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
		`INSERT INTO message(guid, client_id, message_id, topic, ssid, ssid_len, ttl_until, qos, payload) 
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		m.GUID, m.ClientID, m.MessageID, m.TopicName, ssidStringArray, len(m.Ssid), m.TTL, m.Qos, string(m.Payload),
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

func (s *Storage) Query(ctx context.Context, topic string, ssid string, opts storage.QueryOptions) ([]topic.Message, error) {
	return nil, nil
}
