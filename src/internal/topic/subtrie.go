package topic

import (
	"context"
	"sync"

	"github.com/zfair/zqtt/src/zerr"
	"github.com/zfair/zqtt/src/zqttpb"
)

// SubscriberKind currently indicates the location of the subscriber node.
type SubscriberKind int8

const (
	SubscriberKindLocal SubscriberKind = iota + 1
	SubscriberKindRemote
)

// Subscriber interface for a local/remote node.
type Subscriber interface {
	ID() uint64
	Kind() SubscriberKind
	SendMessage(context.Context, *zqttpb.Message) error
}

type Subscribers map[uint64]Subscriber

func newSubscribers() Subscribers {
	return make(Subscribers)
}

// Merge two sets of subscribers.
func (s *Subscribers) Merge(from Subscribers) {
	for id, v := range from {
		// Insert a new one or assign an existing one.
		(*s)[id] = v
	}
}

// Add a new subscriber.
func (s *Subscribers) Add(subscriber Subscriber) bool {
	if _, found := (*s)[subscriber.ID()]; !found {
		(*s)[subscriber.ID()] = subscriber
		return true
	}
	return false
}

// Remove a subscriber.
func (s *Subscribers) Remove(subscriber Subscriber) bool {
	if _, found := (*s)[subscriber.ID()]; found {
		delete(*s, subscriber.ID())
		return true
	}
	return false
}

// Size of subscribers.
func (s *Subscribers) Size() int {
	return len(*s)
}

// node of the subscription trie.
type node struct {
	sync.RWMutex
	word     uint64
	parent   *node
	children map[uint64]*node
	subs     Subscribers
}

func (n *node) orphan() {
	// No need to n.RLock() in SubTrie method, since all methods do not modify
	// node.parent and node.word after new nodes.
	if n.parent == nil {
		return
	}
	n.parent.Lock()
	// It's safe to do this even if the key is already absent from the map.
	delete(n.parent.children, n.word)
	if n.parent.subs.Size() == 0 && len(n.parent.children) == 0 {
		n.parent.Unlock()
		// TODO: Avoid recursion.
		n.parent.orphan()
	} else {
		n.parent.Unlock()
	}
}

// SubTrie is the subscription trie.
type SubTrie struct {
	sync.RWMutex
	root *node
}

// NewSubTrie creates a new subscription trie.
func NewSubTrie() *SubTrie {
	return &SubTrie{
		root: &node{
			subs:     newSubscribers(),
			children: make(map[uint64]*node),
		},
	}
}

// Subscribe a specific topic by SSID.
func (t *SubTrie) Subscribe(ssid []uint64, subscriber Subscriber) error {
	curr := t.root
	for _, word := range ssid {
		curr.RLock()
		child, ok := curr.children[word]
		curr.RUnlock()
		if !ok {
			// Lock write lock.
			curr.Lock()
			// Double check.
			child, ok = curr.children[word]
			if !ok {
				child = &node{
					word:     word,
					subs:     newSubscribers(),
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
	curr.subs.Add(subscriber)
	curr.Unlock()

	return nil
}

// Unsubscribe a topic by SSID.
func (t *SubTrie) Unsubscribe(ssid []uint64, subscriber Subscriber) error {
	curr := t.root
	for _, word := range ssid {
		curr.RLock()
		child, ok := curr.children[word]
		curr.RUnlock()
		if !ok {
			return zerr.ErrSSIDNotFound
		}
		curr = child
	}
	curr.Lock()
	defer curr.Unlock()
	if !curr.subs.Remove(subscriber) {
		return zerr.ErrSubscriberNotFound
	}

	if curr.subs.Size() == 0 && len(curr.children) == 0 {
		// TODO(locustchen): Maybe we can `go curr.orphan()`
		curr.orphan()
	}
	return nil
}

// Lookup the subscribers on a specific topic.
func (t *SubTrie) Lookup(ssid []uint64) Subscribers {
	subs := newSubscribers()
	t.doLookup(t.root, ssid, subs)
	return subs
}

func (t *SubTrie) doLookup(n *node, query []uint64, subs Subscribers) {
	n.RLock()
	defer n.RUnlock()
	if len(query) == 0 {
		subs.Merge(n.subs)
		return
	}

	// Fetch multi-wildcard node.
	if mwNode, ok := n.children[MultiWildcardHash]; ok {
		mwNode.RLock()
		subs.Merge(mwNode.subs)
		mwNode.RUnlock()
	}

	// DFS lookup single wildcard.
	if swNode, ok := n.children[SingleWildcardHash]; ok {
		// TODO: Avoid recursion.
		t.doLookup(swNode, query[1:], subs)
	}

	if matchNode, ok := n.children[query[0]]; ok {
		// TODO: Avoid recursion.
		t.doLookup(matchNode, query[1:], subs)
	}
}
