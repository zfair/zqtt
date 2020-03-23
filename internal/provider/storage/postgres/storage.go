package postgres

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/lib/pq"
	"github.com/zfair/zqtt/internal/provider/storage"
	"github.com/zfair/zqtt/internal/topic"
)

var _ storage.Storage = new(PostgresStorage)

type PostgresStorage struct {
	db *sql.DB
}

func NewPostgresStorage() *PostgresStorage {
	return &PostgresStorage{}
}

func (s *PostgresStorage) Name() string {
	return "postgres"
}

func (s *PostgresStorage) Configure(config map[string]interface{}) error {
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
	// TODO: SetMaxIdelConn and SetMaxOpenConn
	s.db = db
	return nil
}

func (s *PostgresStorage) Close() error {
	return s.db.Close()
}

func (s *PostgresStorage) Store(m *topic.Message) error {
	return nil
}

func (s *PostgresStorage) Query(topic string, ssid string, opts storage.QueryOptions) ([]topic.Message, error) {
	return nil, nil
}
