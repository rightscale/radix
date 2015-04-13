package redis

import (
	"sync/atomic"
	"time"
)

// AtomicDuration stores a Duration that can be changed atomically
// and accessed in a threadsafe way.
type AtomicDuration struct {
	value int64
}

func NewAtomicDuration(duration time.Duration) *AtomicDuration {
	return &AtomicDuration{duration.Nanoseconds()}
}

func (ad *AtomicDuration) Update(duration time.Duration) {
	atomic.SwapInt64(&ad.value, duration.Nanoseconds())
}

func (ad *AtomicDuration) Value() time.Duration {
	return time.Duration(atomic.LoadInt64(&ad.value))
}
