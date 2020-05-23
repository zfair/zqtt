package broker

import (
	"sync"

	"github.com/zfair/zqtt/src/zerr"
)

const minMessageID uint16 = 1
const maxMessageID uint16 = 65535

type MessageIDRing struct {
	sync.Mutex
	currentID uint16
	index     map[uint16]bool
}

func NewMessageIDRing() *MessageIDRing {
	return &MessageIDRing{
		currentID: minMessageID,
		index:     make(map[uint16]bool),
	}
}

func (r *MessageIDRing) GetID() (uint16, error) {
	r.Lock()
	defer r.Unlock()
	// for loop find next valid message id
	for i := r.currentID; i <= maxMessageID && i != 0; i++ {
		if _, ok := r.index[i]; !ok {
			r.index[i] = true
			r.currentID = i
			return i, nil
		}
	}
	// again
	for i := minMessageID; i <= r.currentID && i != 0; i++ {
		if _, ok := r.index[i]; !ok {
			r.index[i] = true
			r.currentID = i
			return i, nil
		}
	}
	return 0, zerr.ErrNoMessageIDAvailable
}

func (r *MessageIDRing) FreeID(mid uint16) {
	r.Lock()
	delete(r.index, mid)
	r.Unlock()
}
