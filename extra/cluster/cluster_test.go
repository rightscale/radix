package cluster

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"strings"
	. "testing"

	"github.com/fzzy/radix/redis"
)

// These tests assume there is a cluster running on ports 7000 and 7001, with
// the first half of the slots assigned to 7000 and the second half assigned to
// 7001. Calling `make up` inside of extra/cluster/testconfs will set this up
// for you. This is automatically done if you are just calling `make test` in
// the project root.
//
// It is also assumed that there is an unrelated redis instance on port 6379,
// which will be connected to but not modified in any way

func TestKeyFromArg(t *T) {
	m := map[string]interface{}{
		"foo0": "foo0",
		"foo1": []byte("foo1"),
		"1":    1,
		"1.1":  1.1,
		"foo2": []string{"foo2", "bar"},
		"foo3": [][]string{{"foo3", "bar"}, {"baz", "buz"}},
	}

	for out, in := range m {
		key, err := keyFromArg(in)
		assert.Nil(t, err)
		assert.Equal(t, out, key)
	}
}

func getCluster(t *T) *Cluster {
	cluster, err := NewCluster("127.0.0.1:7000")
	if err != nil {
		t.Fatal(err)
	}
	return cluster
}

func TestReset(t *T) {
	// Simply initializing a cluster proves Reset works to some degree, since
	// NewCluster calls Reset
	cluster := getCluster(t)
	oldFallback := cluster.FallbackClient
	old7000 := cluster.clients["127.0.0.1:7000"]
	old7001 := cluster.clients["127.0.0.1:7001"]

	// We make a bogus client and add it to the cluster to prove that it gets
	// removed, since it's not needed
	client, err := redis.Dial("tcp", "127.0.0.1:6379")
	assert.Nil(t, err)
	cluster.clients["127.0.0.1:6379"] = client

	// We also change the FallbackAddr to prove that that doesn't break things
	cluster.FallbackAddr = "127.0.0.1:7001"

	err = cluster.Reset()
	assert.Nil(t, err)

	// Prove that the bogus client and old fallback client are closed, and that
	// the new fallback client is open
	closedErr := errors.New("use of closed network connection")
	assert.Equal(t, closedErr, client.Cmd("PING").Err)
	assert.Equal(t, closedErr, oldFallback.Cmd("PING").Err)
	s, err := cluster.FallbackClient.Cmd("PING").Str()
	assert.Nil(t, err)
	assert.Equal(t, "PONG", s)

	// Prove that the remaining two addresses are still in clients, were not
	// reconnected, and still work
	assert.Equal(t, 2, len(cluster.clients))
	assert.Equal(t, old7000, cluster.clients["127.0.0.1:7000"])
	assert.Equal(t, old7001, cluster.clients["127.0.0.1:7001"])
	assert.NotNil(t, cluster.clients["127.0.0.1:7000"])
	assert.NotNil(t, cluster.clients["127.0.0.1:7001"])
	assert.Nil(t, cluster.Cmd("GET", "foo").Err)
	assert.Nil(t, cluster.Cmd("GET", "bar").Err)
}

func TestCmd(t *T) {
	cluster := getCluster(t)
	assert.Nil(t, cluster.Cmd("SET", "foo", "bar").Err)
	assert.Nil(t, cluster.Cmd("SET", "bar", "foo").Err)

	s, err := cluster.Cmd("GET", "foo").Str()
	assert.Nil(t, err)
	assert.Equal(t, "bar", s)

	s, err = cluster.Cmd("GET", "bar").Str()
	assert.Nil(t, err)
	assert.Equal(t, "foo", s)

	assert.Equal(t, 0, cluster.Misses)
}

func TestCmdMiss(t *T) {
	cluster := getCluster(t)
	// foo and bar are on different nodes in our configuration. We set foo to
	// something, then try to retrieve it with a client pointed at a different.
	// It should be redirected and returned correctly

	assert.Nil(t, cluster.Cmd("SET", "foo", "baz").Err)

	barClient, err := cluster.ClientForKey("bar")
	assert.Nil(t, err)

	s, err := cluster.clientCmd(barClient, "GET", "foo").Str()
	assert.Nil(t, err)
	assert.Equal(t, "baz", s)

	assert.Equal(t, 1, cluster.Misses)
}

// This one is kind of a toughy. We have to set a certain slot (which isn't
// being used anywhere else) to be migrating, and test that it does the right
// thing. We'll use a key which isn't set so that we don't have to actually
// migrate the key to get an ASK response
func TestCmdAsk(t *T) {
	cluster := getCluster(t)
	key := "wat"
	slot := CRC16([]byte(key)) % NUM_SLOTS

	assert.Nil(t, cluster.Cmd("DEL", key).Err)
	assert.Equal(t, 0, cluster.Misses)

	// the key "wat" originally belongs on 7000
	src, err := cluster.clientForAddr("127.0.0.1:7000")
	assert.Nil(t, err)
	dst, err := cluster.clientForAddr("127.0.0.1:7001")
	assert.Nil(t, err)

	// We need the node ids. Unfortunately, this is the best way to get them
	nodes, err := src.Cmd("CLUSTER", "NODES").Str()
	assert.Nil(t, err)
	lines := strings.Split(nodes, "\n")
	var srcId, dstId string
	for _, line := range lines {
		id := strings.Split(line, " ")[0]
		if id == "" {
			continue
		}
		if strings.Index(line, "myself,") > -1 {
			srcId = id
		} else {
			dstId = id
		}
	}

	// Start the "migration"
	assert.Nil(t, dst.Cmd("CLUSTER", "SETSLOT", slot, "IMPORTING", srcId).Err)
	assert.Nil(t, src.Cmd("CLUSTER", "SETSLOT", slot, "MIGRATING", dstId).Err)
	assert.Equal(t, 0, cluster.Misses)

	// Make sure we can still "get" the value, and that the redirect actually
	// happened
	assert.Equal(t, redis.NilReply, cluster.Cmd("GET", key).Type)
	assert.Equal(t, 1, cluster.Misses)

	// Bail on the migration TODO this doesn't totally bail for some reason
	assert.Nil(t, dst.Cmd("CLUSTER", "SETSLOT", slot, "NODE", srcId).Err)
	assert.Nil(t, src.Cmd("CLUSTER", "SETSLOT", slot, "NODE", srcId).Err)
}

func TestPipe(t *T) {
	cluster := getCluster(t)

	// We do this multiple times to prove that pipe state gets properly cleaned
	// up after every GetPipeReplies
	for i := 0; i < 5; i++ {
		cluster.PipeAppend("MULTI")
		cluster.PipeAppend("SET", "{foo}abc", "foo")
		cluster.PipeAppend("SET", "{foo}def", "foo")
		cluster.PipeAppend("SET", "{foo}ghi", "foo")
		cluster.PipeAppend("GET", "{foo}abc")
		cluster.PipeAppend("EXEC")

		r := cluster.GetPipeReplies()
		assert.Equal(t, 6, len(r))

		s, err := r[0].Str()
		assert.Nil(t, err)
		assert.Equal(t, "OK", s)

		for i := 1; i < 5; i++ {
			s, err := r[i].Str()
			assert.Nil(t, err, "i: %d", i)
			assert.Equal(t, "QUEUED", s, "i: %d", i)
		}

		assert.Equal(t, 4, len(r[5].Elems))
		r = r[5].Elems

		for i := 0; i < 3; i++ {
			s, err := r[i].Str()
			assert.Nil(t, err, "i: %d", i)
			assert.Equal(t, "OK", s, "i: %d", i)
		}

		s, err = r[3].Str()
		assert.Nil(t, err)
		assert.Equal(t, "foo", s)
	}
}

func TestErrPipe(t *T) {
	// We're going to mess with the cluster a bit, specifically for the key
	// "woot" which lies in slot 261 and ought to exist on the port 7000 node
	cluster := getCluster(t)
	cluster.mapping[261] = "127.0.0.1:7001"

	cluster.PipeAppend("MULTI")
	cluster.PipeAppend("SET", "woot", "woot!")
	cluster.PipeAppend("EXEC")

	r := cluster.GetPipeReplies()
	assert.Equal(t, 3, len(r))

	s, err := r[0].Str()
	assert.Nil(t, err)
	assert.Equal(t, "OK", s)

	assert.NotNil(t, r[1].Err)
	assert.NotNil(t, r[2].Err)
}
