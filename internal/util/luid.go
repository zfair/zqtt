package util

import (
	"sync/atomic"
	"time"
)

// ID represents a process-wide unique ID.
type LUID uint64

// next is the next identifier. We seed it with the time in seconds
// to avoid collisions of ids between process restarts.
var next = uint64(
	time.Now().Sub(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)).Seconds(),
)

// NewID generates a new, process-wide unique ID.
func NewLUID() LUID {
	return LUID(atomic.AddUint64(&next, 1))
}
