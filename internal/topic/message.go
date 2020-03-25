package topic

const SingleWildcard = "+"
const MultiWildcard = "#"

var (
	SingleWildcardHash = Sum64([]byte(SingleWildcard))
	MultiWildcardHash  = Sum64([]byte(MultiWildcard))
)

type Message struct {
	MessageSeq int64
	GUID       string // internal unique id of this message
	ClientID   string
	MessageID  uint16 // the origin id of this message (optioal)
	TopicName  string // the origin topic name of this message
	Ssid       []uint64
	Qos        byte // qos of this message
	TTLUntil   int64
	Payload    []byte
}

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
		TTLUntil:  ttlUntil,
		Payload:   payload,
	}
}

func (m *Message) SetMessageSeq(messageSeq int64) {
	m.MessageSeq = messageSeq
}
