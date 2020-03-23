package topic

type Message struct {
	MessageSeq int64
	GUID       string // internal unique id of this message
	ClientID   string
	MessageID  uint16 // the origin id of this message (optioal)
	Topic      string // the origin topic name of this message
	Ssid       []uint64
	Qos        byte // qos of this message
	TTL        int
	Payload    []byte
}

func NewMessage(
	messageSeq int64,
	guid string,
	clientID string,
	messageID uint16,
	topic string,
	ssid []uint64,
	qos byte,
	ttl int,
	payload []byte,
) *Message {
	return &Message{
		MessageSeq: messageSeq,
		GUID:       guid,
		ClientID:   clientID,
		MessageID:  messageID,
		Topic:      topic,
		Ssid:       ssid,
		Qos:        qos,
		TTL:        ttl,
		Payload:    payload,
	}
}
