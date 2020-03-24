package topic

type Message struct {
	MessageSeq int64
	GUID       string // internal unique id of this message
	ClientID   string
	MessageID  uint16 // the origin id of this message (optioal)
	TopicName  string // the origin topic name of this message
	Ssid       []uint64
	Qos        byte // qos of this message
	TTL        int
	Payload    []byte
}

func NewMessage(
	guid string,
	clientID string,
	messageID uint16,
	topicName string,
	ssid []uint64,
	qos byte,
	ttl int,
	payload []byte,
) *Message {
	return &Message{
		GUID:      guid,
		ClientID:  clientID,
		MessageID: messageID,
		TopicName: topicName,
		Ssid:      ssid,
		Qos:       qos,
		TTL:       ttl,
		Payload:   payload,
	}
}

func (m *Message) SetMessageSeq(messageSeq int64) {
	m.MessageSeq = messageSeq
}
