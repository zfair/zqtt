package topic

type Message struct {
	ID      string // internal unique id of this message
	Topic   string // the origin topic name of this message
	Ssid    []uint64
	Payload []byte
}

func NewMessage(id string, topic string, ssid []uint64, payload []byte) *Message {
	return &Message{
		ID:      id,
		Topic:   topic,
		Ssid:    ssid,
		Payload: payload,
	}
}

func NewPayloadOnlyMessage(payload []byte) *Message {
	return &Message{
		Payload: payload,
	}
}
