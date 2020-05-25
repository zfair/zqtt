package topic

const (
	SingleWildcard = "+"
	MultiWildcard  = "#"
)

var (
	SingleWildcardHash = Sum64([]byte(SingleWildcard))
	MultiWildcardHash  = Sum64([]byte(MultiWildcard))
)

// SSID is the subscription ID
type SSID = []uint64

type part interface {
	// GetValue gets the value from the topic part.
	GetValue() interface{}
}

type partName struct{ value string }

func (p partName) GetValue() interface{} { return p.value }

type partSingleWildcard struct{}

func (partSingleWildcard) GetValue() interface{} { return nil }

type partMultiWildcard struct{}

func (partMultiWildcard) GetValue() interface{} { return nil }

type option struct {
	Key   string
	Value string
}

type TopicKind int8

const (
	TopicKindStatic   TopicKind = iota + 1 // topic without wildcard
	TopicKindWildcard                      // topic with wildcard
)

// Topic contains full info from a topic string, e.g. the subscription options
// and potential wildcards to match.  It is used to generate SSIDs and further
// process the options.
type Topic struct {
	kind      TopicKind
	topicName string
	parts     []part
	options   map[string]string
}

// Topic converts to SSID.
func (t *Topic) ToSSID() SSID {
	var ret SSID

	for _, p := range t.parts {
		switch v := p.(type) {
		case partName:
			value := []byte(v.GetValue().(string))
			ret = append(ret, Sum64(value))
		case partSingleWildcard:
			ret = append(ret, SingleWildcardHash)
		case partMultiWildcard:
			ret = append(ret, MultiWildcardHash)
		}
	}

	return ret
}

// Topic converts to SSID.
func (t *Topic) Kind() TopicKind {
	return t.kind
}

func (t *Topic) TopicName() string {
	return t.topicName
}
