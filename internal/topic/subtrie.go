package topic

import (
	"sync"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/zfair/zqtt/internal/util"
)

type SubscriberType int8

const (
	SubscriberTypeLocal int8 = iota + 1
	SubscriberTypeRemote
)

type Subscriber interface {
	ID() util.LUID
	Type() SubscriberType
	Send(packets.ControlPacket) error
}

type Subscribers map[util.LUID]Subscriber

type node struct {
	sync.RWMutex
	word     uint64
	parent   *node
	children map[uint64]*node
	subs     Subscribers
}
type SubTrie struct {
	sync.RWMutex
	root *node
}

func NewSubTrie() *SubTrie {
	return &SubTrie{
		root: &node{
			subs:     make(Subscribers),
			children: make(map[uint64]*node),
		},
	}
}

func (t *SubTrie) Subscribe(ssid []uint64, subscriber Subscriber) error {
	curr := t.root
	for _, word := range ssid {
		curr.RLock()
		child, ok := curr.children[word]
		curr.RUnlock()
		if !ok {
			child = &node{
				word:     word,
				subs:     make(Subscribers),
				parent:   curr,
				children: make(map[uint64]*node),
			}
			// unlock read lock
			// lock write lock
			curr.Lock()
			curr.children[word] = child
			curr.Unlock()
		}
		curr = child
	}

	curr.Lock()
	if _, found := curr.subs[subscriber.ID()]; !found {
		curr.subs[subscriber.ID()] = subscriber
	}
	curr.Unlock()

	return nil
}
