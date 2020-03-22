package postgres

import (
	"github.com/zfair/zqtt/internal/provider/storage"
)

var _ storage.Storage = new(PostgresStorage)

type PostgresStorage struct {
}

func NewPostgresStorage() *PostgresStorage {

}
