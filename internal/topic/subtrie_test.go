package topic

import (
	"strings"
	"testing"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/spaolacci/murmur3"
	"github.com/stretchr/testify/assert"
)

func parseTopic(topic string) []uint64 {
	parts := strings.Split(topic, "/")
	ssid := make([]uint64, len(parts))
	for i, part := range parts {
		v := murmur3.Sum64([]byte(part))
		ssid[i] = v
	}
	return ssid
}

func toTestID(topic string) uint64 {
	return murmur3.Sum64([]byte(topic))
}

type testSubscriber struct {
	id uint64
}

func newTestSubscriber(id uint64) *testSubscriber {
	return &testSubscriber{
		id,
	}
}

func (s *testSubscriber) ID() uint64 {
	return s.id
}

func (s *testSubscriber) Type() SubscriberType {
	return SubscriberTypeLocal
}

func (s *testSubscriber) Send(packets.ControlPacket) error {
	return nil
}

type testCase struct {
	lookupSSID []uint64
	matchCount int
	matchIDs   []uint64
}

func TestSubTrie(t *testing.T) {
	assert := assert.New(t)
	topics := []string{
		"#",
		"+",
		"hello/#",
		"hello/+",
		"hello/+/zqtt",
		"hello/mqtt/#",
		"hello/mqtt/+",
		"hello/mqtt/zqtt",
		"hello/mqtt/+/+",
		"hello/mqtt/+/foo",
		"hello/mqtt/zqtt/foo",
	}

	testCases := []testCase{
		{
			lookupSSID: parseTopic("a"),
			matchCount: 2,
			matchIDs: []uint64{
				toTestID("#"),
				toTestID("+"),
			},
		},
		{
			lookupSSID: parseTopic("a/b"),
			matchCount: 1,
			matchIDs: []uint64{
				toTestID("#"),
			},
		},
	}

	// construct test sub trie
	trie := NewSubTrie()
	for _, topic := range topics {
		ssid := parseTopic(topic)
		subscriberID := toTestID(topic)
		trie.Subscribe(ssid, newTestSubscriber(subscriberID))
	}

	for _, c := range testCases {
		subs := trie.Lookup(c.lookupSSID)
		assert.Equal(c.matchCount, subs.Size())
	}
}
