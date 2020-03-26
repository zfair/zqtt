package topic

const (
	SingleWildcard = "+"
	MultiWildcard  = "#"
)

var (
	SingleWildcardHash = Sum64([]byte(SingleWildcard))
	MultiWildcardHash  = Sum64([]byte(MultiWildcard))
)

// Message on a specific topic.
type Message struct {
	MessageSeq int64
	// Internally global unique ID of this message.
	GUID     string
	ClientID string
	// Original ID of this message (optional).
	MessageID uint16
	// Original topic name of this message.
	TopicName string
	Ssid      []uint64
	// QoS of this message.
	Qos      byte
	TtlUntil int64
	Payload  []byte
}

// NewMessage creates a new message.
func NewMessage(
	guid string,
	clientID string,
	messageID uint16,
	topicName string,
	ssid []uint64,
	qos byte,
	ttlUntil int64,
	payload []byte,
) *Message {
	return &Message{
		GUID:      guid,
		ClientID:  clientID,
		MessageID: messageID,
		TopicName: topicName,
		Ssid:      ssid,
		Qos:       qos,
		TtlUntil:  ttlUntil,
		Payload:   payload,
	}
}

// SetMessageSeq sets the sequence number of the message.
func (m *Message) SetMessageSeq(messageSeq int64) {
	m.MessageSeq = messageSeq
}
