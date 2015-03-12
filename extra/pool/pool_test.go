package pool

import (
	"github.com/rightscale/radix/redis"
	. "testing"
)

func TestPool(t *T) {
	pool, err := NewPool("tcp", "localhost:6379", 10, 10)
	if err != nil {
		t.Fatal(err)
	}

	conns := make([]*redis.Client, 20)
	for i := range conns {
		if conns[i], err = pool.Get(); err != nil {
			t.Fatal(err)
		}
	}

	for i := range conns {
		pool.Put(conns[i])
	}

	pool.Empty()
}

func TestPoolInitialConnections(t *T) {
	pool, err := NewPool("tcp", "localhost:6379", 2, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pool.pool) != 2 || cap(pool.pool) != 10 {
		t.Error("it should only open the specified number of initial connections")
	}

	pool.Empty()
}
