package util

import (
	"sync/atomic"
	"time"
)

// next is the next identifier. We seed it with the time in seconds
// to avoid collisions of IDs between process restarts.
var next = uint64(
	time.Now().Sub(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)).Seconds(),
)

// NewLUID generates a new, process-wide unique ID.
func NewLUID() uint64 {
	return atomic.AddUint64(&next, 1)
}
