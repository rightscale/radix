package sentinel

import (
	"sync/atomic"
)

// AtomicFlag stores a boolean that can be changed atomically
// and accessed in a threadsafe way.
type AtomicFlag struct {
	value uint32 // 0 is false, 1 is true
}

func (af *AtomicFlag) IsSet() bool {
	return atomic.LoadUint32(&af.value) == 1
}

func (af *AtomicFlag) Set() {
	atomic.SwapUint32(&af.value, 1)
}

func (af *AtomicFlag) Unset() {
	atomic.SwapUint32(&af.value, 0)
}
