package postgres

import (
	"context"
	"fmt"
	"strings"
)

// we build postgres index on topic parts
const maxTopicParts = 8

var validConfigKeywords = []string{
	"dbname",
	"user",
	"password",
	"host",
	"port",
	"sslmode",
	"connect_timeout",
}

func generateConnString(ctx context.Context, config map[string]interface{}) (string, error) {
	var sb strings.Builder

	for _, key := range validConfigKeywords {
		if value, ok := config[key]; ok {
			cfgPart := fmt.Sprintf("%s=%s ", key, value.(string))
			if _, err := sb.WriteString(cfgPart); err != nil {
				return "", err
			}
		}
	}

	connStr := sb.String()
	return connStr, nil
}
