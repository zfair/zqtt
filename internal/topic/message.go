package topic

type Message struct {
	GUID      string // internal unique id of this message
	MessageID uint16 // the origin id of this message (optioal)
	Qos       byte   // qos of this message
	Topic     string // the origin topic name of this message
	Ssid      []uint64
	Payload   []byte
}

func NewMessage(
	guid string,
	messageID uint16,
	qos byte,
	topic string,
	ssid []uint64,
	payload []byte,
) *Message {
	return &Message{
		GUID:      guid,
		MessageID: messageID,
		Qos:       qos,
		Topic:     topic,
		Ssid:      ssid,
		Payload:   payload,
	}
}
