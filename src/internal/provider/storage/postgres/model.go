package postgres

import (
	"github.com/lib/pq"
	"time"
)

type messageModel struct {
	MessageSeq int64
	GUID       string
	ClientID   string
	MessageID  int
	TopicName  string
	Ssid       pq.StringArray
	SsidLen    int
	TtlUntil   int64
	Qos        int
	Payload    string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}
