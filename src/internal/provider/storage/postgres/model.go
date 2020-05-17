package postgres

import (
	"time"

	"github.com/lib/pq"
)

type messageModel struct {
	MessageSeq time.Time
	GUID       string
	ClientID   string
	TopicName  string
	Ssid       pq.StringArray
	SsidLen    int
	TTLUntil   time.Time
	Qos        int
	Payload    string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}
