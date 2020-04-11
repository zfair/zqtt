package topic

import "time"

// Message on a specific topic.
type Message struct {
	messageSeq int64
	// Internally global unique ID of this message.
	GUID     string
	ClientID string
	// Original ID of this message (optional).
	MessageID uint16
	// Original topic name of this message.
	TopicName string
	Ssid      SSID
	// QoS of this message.
	Qos      byte
	TTLUntil time.Time
	Payload  []byte
}

// NewMessage creates a new message.
func NewMessage(
	guid string,
	clientID string,
	messageID uint16,
	topicName string,
	ssid SSID,
	qos byte,
	ttlUntil time.Time,
	payload []byte,
) *Message {
	return &Message{
		GUID:      guid,
		ClientID:  clientID,
		MessageID: messageID,
		TopicName: topicName,
		Ssid:      ssid,
		Qos:       qos,
		TTLUntil:  ttlUntil,
		Payload:   payload,
	}
}

// SetMessageSeq sets the sequence number of the message.
func (m *Message) SetMessageSeq(messageSeq int64) {
	m.messageSeq = messageSeq
}

// GetMessageSeq get the sequence number of the message.
func (m *Message) GetMessageSeq() int64 {
	return m.messageSeq
}
