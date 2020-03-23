package postgres

import "testing"

// before run this test, you should spawn a postgres process
// docker run -d --name some-postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres
func TestConfigurePostgresStorage(t *testing.T) {
	config := map[string]interface{}{
		"dbname":   "postgres",
		"user":     "postgres",
		"password": "postgres",
		"host":     "127.0.0.1",
		"port":     "5432",
	}
	storage := NewPostgresStorage()
	err := storage.Configure(config)
	if err != nil {
		t.Fatal(err)
	}
}
