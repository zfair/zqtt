package topic

import (
	"strings"
	"testing"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/spaolacci/murmur3"
	"github.com/stretchr/testify/assert"
	"github.com/zfair/zqtt/zerrors"
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

type lookupTestCase struct {
	lookupSSID []uint64
	matchCount int
	matchIDs   []uint64
}

func TestLookup(t *testing.T) {
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

	testCases := []lookupTestCase{
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
		{
			lookupSSID: parseTopic("x/y"),
			matchCount: 1,
			matchIDs: []uint64{
				toTestID("#"),
			},
		},
		{
			lookupSSID: parseTopic("hello/world"),
			matchCount: 3,
			matchIDs: []uint64{
				toTestID("#"),
				toTestID("hello/#"),
				toTestID("hello/+"),
			},
		},
		{
			lookupSSID: parseTopic("hello/world/c"),
			matchCount: 2,
			matchIDs: []uint64{
				toTestID("#"),
				toTestID("hello/#"),
			},
		},
		{
			lookupSSID: parseTopic("hello/mqtt/zqtt"),
			matchCount: 6,
			matchIDs: []uint64{
				toTestID("#"),
				toTestID("hello/#"),
				toTestID("hello/mqtt/+"),
				toTestID("hello/+/zqtt"),
				toTestID("hello/mqtt/#"),
				toTestID("hello/mqtt/+"),
				toTestID("hello/mqtt/zqtt"),
			},
		},
		{
			lookupSSID: parseTopic("hello/mqtt/ohh"),
			matchCount: 4,
			matchIDs: []uint64{
				toTestID("#"),
				toTestID("hello/#"),
				toTestID("hello/mqtt/+"),
				toTestID("hello/mqtt/#"),
			},
		},
		{
			lookupSSID: parseTopic("hello/mqtt/ohh/bilibili"),
			matchCount: 4,
			matchIDs: []uint64{
				toTestID("#"),
				toTestID("hello/#"),
				toTestID("hello/mqtt/#"),
				toTestID("hello/mqtt/+/+"),
			},
		},
		{
			lookupSSID: parseTopic("hello/mqtt/ohh/acfun"),
			matchCount: 4,
			matchIDs: []uint64{
				toTestID("#"),
				toTestID("hello/#"),
				toTestID("hello/mqtt/#"),
				toTestID("hello/mqtt/+/+"),
			},
		},
		{
			lookupSSID: parseTopic("hello/mqtt/bilibili/foo"),
			matchCount: 5,
			matchIDs: []uint64{
				toTestID("#"),
				toTestID("hello/#"),
				toTestID("hello/mqtt/#"),
				toTestID("hello/mqtt/+/+"),
				toTestID("hello/mqtt/+/foo"),
			},
		},
		{
			lookupSSID: parseTopic("hello/mqtt/zqtt/foo"),
			matchCount: 6,
			matchIDs: []uint64{
				toTestID("#"),
				toTestID("hello/#"),
				toTestID("hello/mqtt/#"),
				toTestID("hello/mqtt/+/+"),
				toTestID("hello/mqtt/+/foo"),
				toTestID("hello/mqtt/zqtt/foo"),
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
		for _, id := range c.matchIDs {
			sub, ok := subs[id]
			assert.Equal(true, ok)
			if !ok {
				t.Fatal("subscriber not found")
			}
			assert.Equal(id, sub.ID())
		}
	}
}

type unsubscribeTestCase struct {
	unsubscribeTopic string
	unsubscribeErr   error
	lookupSSID       []uint64
	beforeMatchCount int
	beforeMatchIDs   []uint64
	afterMatchCount  int
	afterMatchIDs    []uint64
}

func TestUnsubscribe(t *testing.T) {
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

	// unsubscribe will effect the result of lookup
	testCases := []unsubscribeTestCase{
		{
			unsubscribeTopic: "#",
			unsubscribeErr:   nil,
			lookupSSID:       parseTopic("a"),
			beforeMatchCount: 2,
			beforeMatchIDs: []uint64{
				toTestID("#"),
				toTestID("+"),
			},
			afterMatchCount: 1,
			afterMatchIDs: []uint64{
				toTestID("+"),
			},
		},
		{
			// subscriber of "#" topic has been unsubscribe
			unsubscribeTopic: "#",
			unsubscribeErr:   zerrors.ErrSSIDNotFound,
			lookupSSID:       parseTopic("a"),
			beforeMatchCount: 1,
			beforeMatchIDs: []uint64{
				toTestID("+"),
			},
			afterMatchCount: 1,
			afterMatchIDs: []uint64{
				toTestID("+"),
			},
		},
		{
			unsubscribeTopic: "+",
			unsubscribeErr:   nil,
			lookupSSID:       parseTopic("a"),
			beforeMatchCount: 1,
			beforeMatchIDs: []uint64{
				toTestID("+"),
			},
			afterMatchCount: 0,
			afterMatchIDs:   []uint64{},
		},
		{
			// subscriber of "#" topic has been unsubscribe
			unsubscribeTopic: "#",
			unsubscribeErr:   zerrors.ErrSSIDNotFound,
			lookupSSID:       parseTopic("a/b"),
			beforeMatchCount: 0,
			beforeMatchIDs:   []uint64{},
			afterMatchCount:  0,
			afterMatchIDs:    []uint64{},
		},
		{
			// subscriber of "#" topic has been unsubscribe
			unsubscribeTopic: "#",
			unsubscribeErr:   zerrors.ErrSSIDNotFound,
			lookupSSID:       parseTopic("hello/world"),
			beforeMatchCount: 2,
			beforeMatchIDs: []uint64{
				toTestID("hello/#"),
				toTestID("hello/+"),
			},
			afterMatchCount: 2,
			afterMatchIDs: []uint64{
				toTestID("hello/#"),
				toTestID("hello/+"),
			},
		},
		{
			unsubscribeTopic: "hello/#",
			unsubscribeErr:   nil,
			lookupSSID:       parseTopic("hello/world"),
			beforeMatchCount: 2,
			beforeMatchIDs: []uint64{
				toTestID("hello/#"),
				toTestID("hello/+"),
			},
			afterMatchCount: 1,
			afterMatchIDs: []uint64{
				toTestID("hello/+"),
			},
		},
		{
			unsubscribeTopic: "hello/+",
			unsubscribeErr:   nil,
			lookupSSID:       parseTopic("hello/world"),
			beforeMatchCount: 1,
			beforeMatchIDs: []uint64{
				toTestID("hello/+"),
			},
			afterMatchCount: 0,
			afterMatchIDs:   []uint64{},
		},
		{
			unsubscribeTopic: "hello/+",
			unsubscribeErr:   zerrors.ErrSubscriberNotFound,
			lookupSSID:       parseTopic("hello/world"),
			beforeMatchCount: 0,
			beforeMatchIDs:   []uint64{},
			afterMatchCount:  0,
			afterMatchIDs:    []uint64{},
		},
		{
			unsubscribeTopic: "hello/+",
			unsubscribeErr:   zerrors.ErrSubscriberNotFound,
			lookupSSID:       parseTopic("hello/world/zqtt"),
			beforeMatchCount: 1,
			beforeMatchIDs: []uint64{
				toTestID("hello/+/zqtt"),
			},
			afterMatchCount: 1,
			afterMatchIDs: []uint64{
				toTestID("hello/+/zqtt"),
			},
		},
		{
			unsubscribeTopic: "hello/+/zqtt",
			unsubscribeErr:   nil,
			lookupSSID:       parseTopic("hello/world/zqtt"),
			beforeMatchCount: 1,
			beforeMatchIDs: []uint64{
				toTestID("hello/+/zqtt"),
			},
			afterMatchCount: 0,
			afterMatchIDs:   []uint64{},
		},
		{
			// after remove "hello/+/zqtt", node "hello/+" becomes an orphan
			unsubscribeTopic: "hello/+",
			unsubscribeErr:   zerrors.ErrSSIDNotFound,
			lookupSSID:       parseTopic("hello/world"),
			beforeMatchCount: 0,
			beforeMatchIDs:   []uint64{},
			afterMatchCount:  0,
			afterMatchIDs:    []uint64{},
		},
		{
			// repeat
			unsubscribeTopic: "hello/+",
			unsubscribeErr:   zerrors.ErrSSIDNotFound,
			lookupSSID:       parseTopic("hello/world"),
			beforeMatchCount: 0,
			beforeMatchIDs:   []uint64{},
			afterMatchCount:  0,
			afterMatchIDs:    []uint64{},
		},
		{
			unsubscribeTopic: "hello/mqtt/#",
			unsubscribeErr:   nil,
			lookupSSID:       parseTopic("hello/mqtt/ohh"),
			beforeMatchCount: 2,
			beforeMatchIDs: []uint64{
				toTestID("hello/mqtt/#"),
				toTestID("hello/mqtt/+"),
			},
			afterMatchCount: 1,
			afterMatchIDs: []uint64{
				toTestID("hello/mqtt/+"),
			},
		},
		{
			unsubscribeTopic: "hello/mqtt/+",
			unsubscribeErr:   nil,
			lookupSSID:       parseTopic("hello/mqtt/ohh"),
			beforeMatchCount: 1,
			beforeMatchIDs: []uint64{
				toTestID("hello/mqtt/+"),
			},
			afterMatchCount: 0,
			afterMatchIDs:   []uint64{},
		},
		{
			// repeat
			unsubscribeTopic: "hello/mqtt/+",
			unsubscribeErr:   zerrors.ErrSubscriberNotFound,
			lookupSSID:       parseTopic("hello/mqtt/ohh"),
			beforeMatchCount: 0,
			beforeMatchIDs:   []uint64{},
			afterMatchCount:  0,
			afterMatchIDs:    []uint64{},
		},
		{
			unsubscribeTopic: "hello/mqtt/#",
			unsubscribeErr:   zerrors.ErrSSIDNotFound,
			lookupSSID:       parseTopic("hello/mqtt/ohh"),
			beforeMatchCount: 0,
			beforeMatchIDs:   []uint64{},
			afterMatchCount:  0,
			afterMatchIDs:    []uint64{},
		},
		{
			unsubscribeTopic: "hello/mqtt/#",
			unsubscribeErr:   zerrors.ErrSSIDNotFound,
			lookupSSID:       parseTopic("hello/mqtt/bilibili/acfun"),
			beforeMatchCount: 1,
			beforeMatchIDs: []uint64{
				toTestID("hello/mqtt/+/+"),
			},
			afterMatchCount: 1,
			afterMatchIDs: []uint64{
				toTestID("hello/mqtt/+/+"),
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
		// lookup before unsubscribe
		subs := trie.Lookup(c.lookupSSID)
		assert.Equal(c.beforeMatchCount, subs.Size())
		for _, id := range c.beforeMatchIDs {
			t.Logf("subs: %v\n", subs)
			sub, ok := subs[id]
			assert.Equal(true, ok)
			if !ok {
				t.Logf("id: %d\n", toTestID("hello/world/zqtt"))
				t.Fatal("subscriber not found")
			}
			assert.Equal(id, sub.ID())
		}
		// unsubscribe
		err := trie.UnSubscribe(
			parseTopic(c.unsubscribeTopic),
			newTestSubscriber(toTestID(c.unsubscribeTopic)),
		)
		if ok := assert.Equal(c.unsubscribeErr, err); !ok {
			t.Fatal("assert.Equal false")
		}
		// lookup after unsubscribe
		subs = trie.Lookup(c.lookupSSID)
		assert.Equal(c.afterMatchCount, subs.Size())
		for _, id := range c.afterMatchIDs {
			sub, ok := subs[id]
			assert.Equal(true, ok)
			if !ok {
				t.Fatal("subscriber not found")
			}
			assert.Equal(id, sub.ID())
		}
	}
}
