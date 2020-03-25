package topic

import (
	"sync"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/zfair/zqtt/zerrors"
)

type SubscriberType int8

const (
	SubscriberTypeLocal SubscriberType = iota + 1
	SubscriberTypeRemote
)

type Subscriber interface {
	ID() uint64
	Type() SubscriberType
	Send(packets.ControlPacket) error
}

type Subscribers map[uint64]Subscriber

func newSubscrbers() Subscribers {
	return make(Subscribers)
}

func (s *Subscribers) AddRange(from Subscribers) {
	for id, v := range from {
		(*s)[id] = v // This would simply overwrite duplicates
	}
}

func (s *Subscribers) AddSubscriber(subscriber Subscriber) bool {
	if _, found := (*s)[subscriber.ID()]; !found {
		(*s)[subscriber.ID()] = subscriber
		return true
	}
	return false
}

func (s *Subscribers) Remove(subscriber Subscriber) bool {
	if _, found := (*s)[subscriber.ID()]; found {
		delete(*s, subscriber.ID())
		return true
	}
	return false
}

func (s *Subscribers) Size() int {
	return len(*s)
}

type node struct {
	sync.RWMutex
	word     uint64
	parent   *node
	children map[uint64]*node
	subs     Subscribers
}

func (n *node) orphan() {
	// No Need n.RLock()
	// in SubTrie method, all method did not modify node.parent and node.word after new node
	if n.parent == nil {
		return
	}
	n.parent.Lock()
	//  It's safe to do this even if the key is already absent from the map
	delete(n.parent.children, n.word)
	if n.parent.subs.Size() == 0 && len(n.parent.children) == 0 {
		n.parent.Unlock()
		n.parent.orphan()
	} else {
		n.parent.Unlock()
	}
}

type SubTrie struct {
	sync.RWMutex
	root *node
}

func NewSubTrie() *SubTrie {
	return &SubTrie{
		root: &node{
			subs:     newSubscrbers(),
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
			// lock write lock
			curr.Lock()
			// double check
			child, ok = curr.children[word]
			if !ok {
				child = &node{
					word:     word,
					subs:     newSubscrbers(),
					parent:   curr,
					children: make(map[uint64]*node),
				}
				curr.children[word] = child
			}
			curr.Unlock()
		}
		curr = child
	}

	curr.Lock()
	curr.subs.AddSubscriber(subscriber)
	curr.Unlock()

	return nil
}

func (t *SubTrie) UnSubscribe(ssid []uint64, subscriber Subscriber) error {
	curr := t.root
	for _, word := range ssid {
		curr.RLock()
		child, ok := curr.children[word]
		curr.RUnlock()
		if !ok {
			return zerrors.ErrSSIDNotFound
		}
		curr = child
	}
	curr.Lock()
	defer curr.Unlock()
	if !curr.subs.Remove(subscriber) {
		return zerrors.ErrSubscriberNotFound
	}

	if curr.subs.Size() == 0 && len(curr.children) == 0 {
		// TODO: maybe we can go curr.orphan()
		curr.orphan()
	}
	return nil
}

func (t *SubTrie) Lookup(ssid []uint64) Subscribers {
	subs := newSubscrbers()
	t.doLookup(t.root, ssid, subs)
	return subs
}

func (t *SubTrie) doLookup(n *node, query []uint64, subs Subscribers) {
	n.RLock()
	defer n.RUnlock()
	if len(query) == 0 {
		subs.AddRange(n.subs)
		return
	}

	// fetch multi wildcard node
	if mwcn, ok := n.children[MultiWildcardHash]; ok {
		mwcn.RLock()
		subs.AddRange(mwcn.subs)
		mwcn.RUnlock()
	}

	// dfs lookup single wildcard
	if swcn, ok := n.children[SingleWildcardHash]; ok {
		t.doLookup(swcn, query[1:], subs)
	}

	if matchn, ok := n.children[query[0]]; ok {
		t.doLookup(matchn, query[1:], subs)
	}
}
