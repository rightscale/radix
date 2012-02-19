package redis

import (
	"flag"
	. "launchpad.net/gocheck"
	"testing"
	"time"
)

// Hook up gocheck into the gotest runner.
func Test(t *testing.T) {
	TestingT(t)
}

var rd *Client

func setUpTest(c *C) {
	rd = NewClient(Configuration{
		Database: 8,
		Address:  "127.0.0.1:6379"})

	r := rd.Command("flushall")
	if !r.OK() {
		c.Fatalf("setUp FLUSHALL failed: %s", r.Error())
	}
}

func tearDownTest(c *C) {
	r := rd.Command("flushall")
	if !r.OK() {
		c.Fatalf("tearDown FLUSHALL failed: %s", r.Error())
	}
	rd.Close()
}

//* Tests
type S struct{}
type Long struct{}

var long = flag.Bool("long", false, "Include blocking tests")

func init() {
	Suite(&S{})
	Suite(&Long{})
}

func (s *Long) SetUpSuite(c *C) {
	if !*long {
		c.Skip("-long not provided")
	}
}

func (s *S) SetUpTest(c *C) {
	setUpTest(c)
}

func (s *S) TearDownTest(c *C) {
	tearDownTest(c)
}

func (s *Long) SetUpTest(c *C) {
	setUpTest(c)
}

func (s *Long) TearDownTest(c *C) {
	tearDownTest(c)
}

// Test Select.
func (s *S) TestSelect(c *C) {
	rd.Select(9)
	c.Check(rd.configuration.Database, Equals, 9)
	rd.Command("set", "foo", "bar")

	rdA := NewClient(Configuration{Database: 9})
	c.Check(rdA.Command("get", "foo").Str(), Equals, "bar")
}

// Test connection commands.
func (s *S) TestConnection(c *C) {
	c.Check(rd.Command("echo", "Hello, World!").Str(), Equals, "Hello, World!")
	c.Check(rd.Command("ping").Str(), Equals, "PONG")
}

// Test single return value commands.
func (s *S) TestSimpleValue(c *C) {
	// Simple value commands.
	rd.Command("set", "simple:string", "Hello,")
	rd.Command("append", "simple:string", " World!")
	c.Check(rd.Command("get", "simple:string").Str(), Equals, "Hello, World!")

	rd.Command("set", "simple:int", 10)
	c.Check(rd.Command("incr", "simple:int").Int(), Equals, 11)

	rd.Command("setbit", "simple:bit", 0, true)
	rd.Command("setbit", "simple:bit", 1, true)
	c.Check(rd.Command("getbit", "simple:bit", 0).Bool(), Equals, true)
	c.Check(rd.Command("getbit", "simple:bit", 1).Bool(), Equals, true)

	c.Check(rd.Command("get", "non:existing:key").Type(), Equals, ReplyNil)
	c.Check(rd.Command("exists", "non:existing:key").Bool(), Equals, false)
	c.Check(rd.Command("setnx", "simple:nx", "Test").Bool(), Equals, true)
	c.Check(rd.Command("setnx", "simple:nx", "Test").Bool(), Equals, false)
}

/*
// Test multi return value commands.
func (s *S) TestMultiple(c *C) {
	// Set values first.
	rd.Command("set", "multiple:a", "a")
	rd.Command("set", "multiple:b", "b")
	rd.Command("set", "multiple:c", "c")

	c.Check(
		rd.Command("mget", "multiple:a", "multiple:b", "multiple:c").Strings(),
		Equals,
		[]string{"a", "b", "c"})
}

// Test hash accessing.
func (s *S) TestHash(c *C) {
	//* Single  return value commands.
	rd.Command("hset", "hash:bool", "true:1", 1)
	rd.Command("hset", "hash:bool", "true:2", true)
	rd.Command("hset", "hash:bool", "true:3", "T")
	rd.Command("hset", "hash:bool", "false:1", 0)
	rd.Command("hset", "hash:bool", "false:2", false)
	rd.Command("hset", "hash:bool", "false:3", "FALSE")
	c.Check(rd.Command("hget", "hash:bool", "true:1").Bool(), Equals, true)
	c.Check(rd.Command("hget", "hash:bool", "true:2").Bool(), Equals, true)
	c.Check(rd.Command("hget", "hash:bool", "true:3").Bool(), Equals, true)
	c.Check(rd.Command("hget", "hash:bool", "false:1").Bool(), Equals, false)
	c.Check(rd.Command("hget", "hash:bool", "false:2").Bool(), Equals, false)
	c.Check(rd.Command("hget", "hash:bool", "false:3").Bool(), Equals, false)

	ha := rd.Command("hgetall", "hash:bool").Hash()
	c.Assert(len(ha), Equals, 6)
	c.Check(ha.Bool("true:1"), Equals, true)
	c.Check(ha.Bool("true:2"), Equals, true)
	c.Check(ha.Bool("true:3"), Equals, true)
	c.Check(ha.Bool("false:1"), Equals, false)
	c.Check(ha.Bool("false:2"), Equals, false)
	c.Check(ha.Bool("false:3"), Equals, false)
}

// Test list commands.
func (s *S) TestList(c *C) {
	rd.Command("rpush", "list:a", "one")
	rd.Command("rpush", "list:a", "two")
	rd.Command("rpush", "list:a", "three")
	rd.Command("rpush", "list:a", "four")
	rd.Command("rpush", "list:a", "five")
	rd.Command("rpush", "list:a", "six")
	rd.Command("rpush", "list:a", "seven")
	rd.Command("rpush", "list:a", "eight")
	rd.Command("rpush", "list:a", "nine")
	c.Check(
		rd.Command("lrange", "list:a", 0, -1).Strings(),
		Equals,
		[]string{"one", "two", "three", "four", "five", "six", "seven", "eight", "nine"})
	c.Check(rd.Command("lpop", "list:a").Str(), Equals, "one")

	vs := rd.Command("lrange", "list:a", 3, 6).Values()
	c.Assert(len(vs), Equals, 4)
	c.Check(vs[0].Str(), Equals, "five")
	c.Check(vs[1].Str(), Equals, "six")
	c.Check(vs[2].Str(), Equals, "seven")
	c.Check(vs[3].Str(), Equals, "eight")

	rd.Command("ltrim", "list:a", 0, 3)
	c.Check(rd.Command("llen", "list:a").Int(), Equals, 4)

	rd.Command("rpoplpush", "list:a", "list:b")
	c.Check(rd.Command("lindex", "list:b", 4711).Value(), IsNil)
	c.Check(rd.Command("lindex", "list:b", 0).Str(), Equals, "five")

	rd.Command("rpush", "list:c", 1)
	rd.Command("rpush", "list:c", 2)
	rd.Command("rpush", "list:c", 3)
	rd.Command("rpush", "list:c", 4)
	rd.Command("rpush", "list:c", 5)
	c.Check(rd.Command("lpop", "list:c").Int(), Equals, 1)
}
*/
// Test set commands.
func (s *S) TestSets(c *C) {
	rd.Command("sadd", "set:a", 1)
	rd.Command("sadd", "set:a", 2)
	rd.Command("sadd", "set:a", 3)
	rd.Command("sadd", "set:a", 4)
	rd.Command("sadd", "set:a", 5)
	rd.Command("sadd", "set:a", 4)
	rd.Command("sadd", "set:a", 3)
	c.Check(rd.Command("scard", "set:a").Int(), Equals, 5)
	c.Check(rd.Command("sismember", "set:a", "4").Bool(), Equals, true)
}

// Test argument formatting.
func (s *S) TestArgToRedis(c *C) {
	// string
	rd.Command("set", "foo", "bar")
	c.Check(
		rd.Command("get", "foo").Str(),
		Equals,
		"bar")

	// []byte
	rd.Command("set", "foo2", []byte{'b', 'a', 'r'})
	c.Check(
		rd.Command("get", "foo2").Bytes(),
		Equals,
		[]byte{'b', 'a', 'r'})

	// bool
	rd.Command("set", "foo3", true)
	c.Check(
		rd.Command("get", "foo3").Bool(),
		Equals,
		true)

	// integers
	rd.Command("set", "foo4", 2)
	c.Check(
		rd.Command("get", "foo4").Str(),
		Equals,
		"2")

	// slice
	rd.Command("rpush", "foo5", []int{1, 2, 3})
	foo5strings, err := rd.Command("lrange", "foo5", 0, -1).Strings()
	c.Assert(err, IsNil)
	c.Check(
		foo5strings,
		Equals,
		[]string{"1", "2", "3"})

	// map
	rd.Command("hset", "foo6", "k1", "v1")
	rd.Command("hset", "foo6", "k2", "v2")
	rd.Command("hset", "foo6", "k3", "v3")

	foo6map, err := rd.Command("hgetall", "foo6").Map()
	c.Assert(err, IsNil)
	c.Check(
		foo6map,
		Equals,
		map[string]string{
			"k1": "v1",
			"k2": "v2",
			"k3": "v3",
		})
}

// Test asynchronous commands.
func (s *S) TestAsync(c *C) {
	fut := rd.AsyncCommand("PING")
	r := fut.Reply()
	c.Check(r.Str(), Equals, "PONG")
}

// Test multi-value commands.
func (s *S) TestMulti(c *C) {
	rd.Command("sadd", "multi:set", "one")
	rd.Command("sadd", "multi:set", "two")
	rd.Command("sadd", "multi:set", "three")

	c.Check(rd.Command("smembers", "multi:set").Len(), Equals, 3)
}

// Test multi commands.
func (s *S) TestMultiCommand(c *C) {
	r := rd.MultiCommand(func(mc *MultiCommand) {
		mc.Command("set", "foo", "bar")
		mc.Command("get", "foo")
	})
	c.Assert(r.Type(), Equals, ReplyMulti)
	c.Check(r.At(0).OK(), Equals, true)
	c.Check(r.At(1).Str(), Equals, "bar")

	r = rd.MultiCommand(func(mc *MultiCommand) {
		mc.Command("set", "foo2", "baz")
		mc.Command("get", "foo2")
		rmc := mc.Flush()
		c.Check(rmc.At(0).OK(), Equals, true)
		c.Check(rmc.At(1).Str(), Equals, "baz")
		mc.Command("set", "foo2", "qux")
		mc.Command("get", "foo2")
	})
	c.Assert(r.Type(), Equals, ReplyMulti)
	c.Check(r.At(0).OK(), Equals, true)
	c.Check(r.At(1).Str(), Equals, "qux")
}

// Test simple transactions.
func (s *S) TestTransaction(c *C) {
	r := rd.Transaction(func(mc *MultiCommand) {
		mc.Command("set", "foo", "bar")
		mc.Command("get", "foo")
	})
	c.Assert(r.Type(), Equals, ReplyMulti)
	c.Check(r.At(0).Str(), Equals, "OK")
	c.Check(r.At(1).Str(), Equals, "bar")

	// Flushing transaction
	r = rd.Transaction(func(mc *MultiCommand) {
		mc.Command("set", "foo", "bar")
		mc.Flush()
		mc.Command("get", "foo")
	})
	c.Assert(r.Type(), Equals, ReplyMulti)
	c.Check(r.Len(), Equals, 2)
	c.Check(r.At(0).Str(), Equals, "OK")
	c.Check(r.At(1).Str(), Equals, "bar")
}

// Test succesful complex tranactions.
func (s *S) TestComplexTransaction(c *C) {
	// Succesful transaction.
	r := rd.MultiCommand(func(mc *MultiCommand) {
		mc.Command("set", "foo", "bar")
		mc.Command("watch", "foo")
		rmc := mc.Flush()
		c.Assert(rmc.Type(), Equals, ReplyMulti)
		c.Assert(rmc.Len(), Equals, 2)
		c.Assert(rmc.At(0).OK(), Equals, true)
		c.Assert(rmc.At(1).OK(), Equals, true)

		mc.Command("multi")
		mc.Command("set", "foo", "baz")
		mc.Command("get", "foo")
		mc.Command("brokenfunc")
		mc.Command("exec")
	})
	c.Assert(r.Type(), Equals, ReplyMulti)
	c.Assert(r.Len(), Equals, 5)
	c.Check(r.At(0).OK(), Equals, true)
	c.Check(r.At(1).OK(), Equals, true)
	c.Check(r.At(2).OK(), Equals, true)
	c.Check(r.At(3).OK(), Equals, false)
	c.Assert(r.At(4).Type(), Equals, ReplyMulti)
	c.Assert(r.At(4).Len(), Equals, 2)
	c.Check(r.At(4).At(0).OK(), Equals, true)
	c.Check(r.At(4).At(1).Str(), Equals, "baz")

	// Discarding transaction
	r = rd.MultiCommand(func(mc *MultiCommand) {
		mc.Command("set", "foo", "bar")
		mc.Command("multi")
		mc.Command("set", "foo", "baz")
		mc.Command("discard")
		mc.Command("get", "foo")
	})
	c.Assert(r.Type(), Equals, ReplyMulti)
	c.Assert(r.Len(), Equals, 5)
	c.Check(r.At(0).OK(), Equals, true)
	c.Check(r.At(1).OK(), Equals, true)
	c.Check(r.At(2).OK(), Equals, true)
	c.Check(r.At(3).OK(), Equals, true)
	c.Check(r.At(4).OK(), Equals, true)
	c.Check(r.At(4).Str(), Equals, "bar")
}

// Test asynchronous multi commands.
func (s *S) TestAsyncMultiCommand(c *C) {
	r := rd.AsyncMultiCommand(func(mc *MultiCommand) {
		mc.Command("set", "foo", "bar")
		mc.Command("get", "foo")
	}).Reply()
	c.Assert(r.Type(), Equals, ReplyMulti)
	c.Check(r.At(0).OK(), Equals, true)
	c.Check(r.At(1).Str(), Equals, "bar")
}

// Test simple asynchronous transactions.
func (s *S) TestAsyncTransaction(c *C) {
	r := rd.AsyncTransaction(func(mc *MultiCommand) {
		mc.Command("set", "foo", "bar")
		mc.Command("get", "foo")
	}).Reply()
	c.Assert(r.Type(), Equals, ReplyMulti)
	c.Check(r.At(0).Str(), Equals, "OK")
	c.Check(r.At(1).Str(), Equals, "bar")
}

// Test Subscription.
func (s *S) TestSubscription(c *C) {
	var messages []*Message

	sub, err := rd.Subscription("chan1", "chan2")
	if err != nil {
		c.Errorf("Failed to subscribe: '%v'!", err)
		return
	}

	go func() {
		for msg := range sub.MessageChan {
			c.Log(msg)
			messages = append(messages, msg)
		}
		c.Log("Subscription closed!")
	}()

	c.Check(rd.Command("publish", "chan1", "foo").Int(), Equals, 1)
	sub.Unsubscribe("chan1")
	c.Check(rd.Command("publish", "chan1", "bar").Int(), Equals, 0)
	sub.Close()

	c.Assert(len(messages), Equals, 4)
	c.Check(messages[0].Type, Equals, MessageSubscribe)
	c.Check(messages[0].Channel, Equals, "chan1")
	c.Check(messages[0].Subscriptions, Equals, 1)
	c.Check(messages[1].Type, Equals, MessageSubscribe)
	c.Check(messages[1].Channel, Equals, "chan2")
	c.Check(messages[1].Subscriptions, Equals, 2)
	c.Check(messages[2].Type, Equals, MessageMessage)
	c.Check(messages[2].Channel, Equals, "chan1")
	c.Check(messages[2].Payload, Equals, "foo")
	c.Check(messages[3].Type, Equals, MessageUnsubscribe)
	c.Check(messages[3].Channel, Equals, "chan1")
}

//* Long tests

// Test aborting complex tranactions.
func (s *Long) TestAbortingComplexTransaction(c *C) {
	go func() {
		time.Sleep(time.Second)
		rd.Command("set", "foo", 9)
	}()

	r := rd.MultiCommand(func(mc *MultiCommand) {
		mc.Command("set", "foo", 1)
		mc.Command("watch", "foo")
		mc.Command("multi")
		rmc := mc.Flush()
		c.Assert(rmc.Type(), Equals, ReplyMulti)
		c.Assert(rmc.Len(), Equals, 3)
		c.Assert(rmc.At(0).OK(), Equals, true)
		c.Assert(rmc.At(1).OK(), Equals, true)
		c.Assert(rmc.At(2).OK(), Equals, true)

		time.Sleep(time.Second * 2)
		mc.Command("set", "foo", 2)
		mc.Command("exec")
	})
	c.Assert(r.Type(), Equals, ReplyMulti)
	c.Assert(r.Len(), Equals, 2)
	c.Check(r.At(1).Type(), Equals, ReplyNil)
}

/*
// Test pop.
func (s *Long) TestPop(c *C) {
	fooPush := func(rd *Client) {
		time.Sleep(time.Second)
		rd.Command("lpush", "pop:first", "foo")
	}

	// Set A: no database timeout.
	rdA := NewClient(Configuration{})

	go fooPush(rdA)

	rAA := rdA.Command("blpop", "pop:first", 5)
	kv := rAA.KeyValue()
	c.Check(kv.Value.Str(), Equals, "foo")

	rAB := rdA.Command("blpop", "pop:first", 1)
	c.Check(rAB.OK(), Equals, true)

	// Set B: database with timeout.
	rdB := NewClient(Configuration{})

	rBA := rdB.Command("blpop", "pop:first", 1)
	c.Check(rBA.OK(), Equals, true)
}
*/
// Test illegal databases.
func (s *Long) TestIllegalDatabases(c *C) {
	c.Log("Test selecting an illegal database...")
	rdA := NewClient(Configuration{Database: 4711})
	rA := rdA.Command("ping")
	c.Check(rA.OK(), Equals, false)

	c.Log("Test connecting to an illegal address...")
	rdB := NewClient(Configuration{Address: "192.168.100.100:12345"})
	rB := rdB.Command("ping")
	c.Check(rB.OK(), Equals, false)
}
