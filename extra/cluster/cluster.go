// The cluster package implements an almost drop-in replacement for a normal
// Client which accounts for a redis cluster setup. It will transparently
// redirect requests to the correct nodes, as well as keep track of which slots
// are mapped to which nodes and updating them accordingly so requests can
// remain as fast as possible.
//
// This package will initially call `cluster slots` in order to retrieve an
// initial idea of the topology of the cluster, but other than that will not
// make any other extraneous calls.
package cluster

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/fzzy/radix/redis"
)

const NUM_SLOTS = 16384

type mapping [NUM_SLOTS]string

func errorReply(err error) *redis.Reply {
	return &redis.Reply{Type: redis.ErrorReply, Err: err}
}

var BadCmdNoKey = &redis.CmdError{errors.New("bad command, no key")}

type pipePart struct {
	cmd  string
	args []interface{}
}

type pipeInfo struct {
	key    string
	client *redis.Client
	parts  []pipePart
}

// Cluster wraps a Client and accounts for all redis cluster logic
type Cluster struct {
	pipeInfo
	mapping
	clients map[string]*redis.Client
	timeout time.Duration

	// FallbackAddr is the address which will be connected to make the
	// FallbackClient. It can be changed post-initialization, Reset must be
	// called for the change to take effect though
	FallbackAddr string

	// FallbackClient is the client which is used when retrieving the cluster
	// topology during initialization or Reset, or when it is unknown which node
	// is handling a particular slot
	FallbackClient *redis.Client

	// Number of slot misses. This is incremented everytime a command's reply is
	// a MOVED or ASK message
	Misses uint64
}

// NewCluster will perform the following steps to initialize:
//
// - Connect to the node given in the arguments. This node will be the fallback
// client for the lifetime of this object.
//
// - The fallback client is used to call CLUSTER SLOTS. The return from this is
// used to build a mapping of slot number -> connection. At the same time any
// new connections which need to be made are created here.
//
// - *Cluster is returned
//
// At this point the Cluster has a complete view of the cluster's topology and
// can immediately start performing commands with (theoretically) zero slot
// misses
func NewCluster(addr string) (*Cluster, error) {
	return NewClusterTimeout(addr, time.Duration(0))
}

// Same as NewCluster, but will use timeout as the read/write timeout when
// communicating with cluster nodes
func NewClusterTimeout(addr string, timeout time.Duration) (*Cluster, error) {
	c := Cluster{
		mapping: mapping{},
		clients: map[string]*redis.Client{},
		timeout: timeout,

		FallbackAddr: addr,
	}
	if err := c.Reset(); err != nil {
		return nil, err
	}
	return &c, nil
}

// Reset will re-retrieve the cluster topology and set up/teardown connections
// as necessary. It begins by always closing the FallbackClient and
// re-connecting it, then using that to call CLUSTER SLOTS. The return from that
// is used to re-create the topology, create any missing clients, and close any
// clients which are no longer needed.
func (c *Cluster) Reset() error {
	var err error
	if c.FallbackClient != nil {
		c.FallbackClient.Close()
	}
	c.FallbackClient, err = redis.DialTimeout("tcp", c.FallbackAddr, c.timeout)
	if err != nil {
		return err
	}

	clients := map[string]*redis.Client{
		c.FallbackAddr: c.FallbackClient,
	}

	r := c.FallbackClient.Cmd("CLUSTER", "SLOTS")
	if r.Err != nil {
		return r.Err
	} else if r.Elems == nil || len(r.Elems) < 1 {
		return errors.New("malformed CLUSTER SLOTS response")
	}

	var start, end, port int
	var ip, slotAddr string
	var client *redis.Client
	var ok bool
	for _, slotGroup := range r.Elems {
		if start, err = slotGroup.Elems[0].Int(); err != nil {
			return err
		}
		if end, err = slotGroup.Elems[1].Int(); err != nil {
			return err
		}
		if ip, err = slotGroup.Elems[2].Elems[0].Str(); err != nil {
			return err
		}
		if port, err = slotGroup.Elems[2].Elems[1].Int(); err != nil {
			return err
		}

		// cluster slots returns a blank ip for the node we're currently
		// connected to. I guess the node doesn't know its own ip? I guess that
		// makes sense
		if ip == "" {
			slotAddr = c.FallbackAddr
		} else {
			slotAddr = ip + ":" + strconv.Itoa(port)
		}
		for i := start; i <= end; i++ {
			c.mapping[i] = slotAddr
		}
		if client, ok = c.clients[slotAddr]; ok {
			clients[slotAddr] = client
		} else {
			client, err = redis.DialTimeout("tcp", slotAddr, c.timeout)
			if err != nil {
				return err
			}
			clients[slotAddr] = client
		}
	}

	for addr := range c.clients {
		if _, ok := clients[addr]; !ok {
			c.clients[addr].Close()
		}
	}
	c.clients = clients

	return nil
}

// Cmd performs the given command on the correct cluster node and gives back the
// command's reply. The command *must* have a key parameter (i.e. len(args) >=
// 1). If any MOVED or ASK errors are returned they will be transparently
// handled by this method. This method will also increment the Misses field on
// the Cluster struct whenever a redirection occurs
func (c *Cluster) Cmd(cmd string, args ...interface{}) *redis.Reply {
	if len(args) < 1 {
		return errorReply(BadCmdNoKey)
	}

	key, err := keyFromArg(args[0])
	if err != nil {
		return errorReply(err)
	}

	client, err := c.ClientForKey(key)
	if err != nil {
		return errorReply(err)
	}
	return c.clientCmd(client, cmd, args...)
}

// clientCmd is separated out from Cmd mainly to aid in testing
func (c *Cluster) clientCmd(
	client *redis.Client, cmd string, args ...interface{},
) *redis.Reply {
	r := client.Cmd(cmd, args...)
	if err := r.Err; err != nil {
		msg := err.Error()
		if strings.HasPrefix(msg, "MOVED") {
			c.Misses++
			slot, addr := redirectInfo(msg)
			c.mapping[slot] = addr

			newClient, err := c.clientForAddr(addr)
			if err != nil {
				return errorReply(err)
			}
			return c.clientCmd(newClient, cmd, args...)
		} else if strings.HasPrefix(msg, "ASK") {
			c.Misses++
			_, addr := redirectInfo(msg)
			newClient, err := c.clientForAddr(addr)
			if err != nil {
				return errorReply(err)
			}
			if r := newClient.Cmd("ASKING"); r.Err != nil {
				return r
			}
			return newClient.Cmd(cmd, args...)
		}
	}
	return r
}

func redirectInfo(msg string) (int, string) {
	parts := strings.Split(msg, " ")
	slotStr := parts[1]
	slot, err := strconv.Atoi(slotStr)
	if err != nil {
		// if redis is returning bad integers, we have problems
		panic(err)
	}
	addr := parts[2]
	return slot, addr
}

// We unfortunately support some weird stuff for command arguments, such as
// automatically flattening slices and things like that. So this gets
// complicated. Usually the user will do something normal like pass in a string
// or byte slice though, so usually this will be pretty fast
func keyFromArg(arg interface{}) (string, error) {
	switch argv := arg.(type) {
	case string:
		return argv, nil
	case []byte:
		return string(argv), nil
	default:
		switch reflect.TypeOf(arg).Kind() {
		case reflect.Slice:
			argVal := reflect.ValueOf(arg)
			if argVal.Len() < 1 {
				return "", BadCmdNoKey
			}
			first := argVal.Index(0).Interface()
			return keyFromArg(first)
		case reflect.Map:
			// Maps have no order, we can't possibly choose a key out of one
			return "", BadCmdNoKey
		default:
			return fmt.Sprint(arg), nil
		}
	}
}

// ClientForKey returns the Client which *ought* to handle the given key, based
// on Cluster's understanding of the cluster topology at the given moment (no
// extra commands are issued to any redis instances to support this call). This
// will only return an error if Cluster encountered an error connecting to a
// node which it hadn't previously connected to. If no node is set to handle the
// key's slot than the fallback client is returned
func (c *Cluster) ClientForKey(key string) (*redis.Client, error) {
	if start := strings.Index(key, "{"); start >= 0 {
		if end := strings.Index(key[start+2:], "}"); end >= 0 {
			key = key[start+1 : start+2+end]
		}
	}
	i := CRC16([]byte(key)) % NUM_SLOTS
	addr := c.mapping[i]
	if addr == "" {
		return c.FallbackClient, nil
	}
	return c.clientForAddr(addr)
}

func (c *Cluster) clientForAddr(addr string) (*redis.Client, error) {
	client := c.clients[addr]
	if client != nil {
		return client, nil
	}

	client, err := redis.DialTimeout("tcp", addr, c.timeout)
	if err != nil {
		return nil, err
	}
	c.clients[addr] = client
	return client, nil
}

// PipeAppend adds a command to the buffer of commands which will be sent at the
// same time to a single client. See GetPipeReplies for the full behavior
func (c *Cluster) PipeAppend(cmd string, args ...interface{}) {
	if c.pipeInfo.parts == nil {
		c.pipeInfo.parts = make([]pipePart, 0, 4)
	}
	c.pipeInfo.parts = append(c.pipeInfo.parts, pipePart{cmd, args})

	if len(args) > 0 && c.pipeInfo.key == "" {
		c.pipeInfo.key, _ = keyFromArg(args[0])
	}
}

func pipeRetErr(ret []*redis.Reply, err error) []*redis.Reply {
	for i := range ret {
		ret[i] = errorReply(err)
	}
	return ret
}

// GetPipeReplies sends all the commands in the current pipe buffer (created
// through PipeAppend) to a single client at the same time and returns all of
// their responses. The client is chosen by finding the first command in the
// bufffer which specifies a key, and using that key's client as the destination
// client for all the commands. GetPipeReplies will NOT do transparent
// redirection like the normal Cmd will. The primary use-case of pipelining is
// for MULTI/EXEC transactions.
func (c *Cluster) GetPipeReplies() []*redis.Reply {
	p := c.pipeInfo
	c.pipeInfo = pipeInfo{}

	if p.parts == nil || len(p.parts) == 0 {
		return []*redis.Reply{}
	}

	ret := make([]*redis.Reply, len(p.parts))

	if p.key == "" {
		return pipeRetErr(ret, BadCmdNoKey)
	}

	client, err := c.ClientForKey(p.key)
	if err != nil {
		return pipeRetErr(ret, err)
	}

	for i := range p.parts {
		p := &p.parts[i]
		client.Append(p.cmd, p.args...)
	}

	for i := range ret {
		ret[i] = client.GetReply()
	}

	return ret
}

// Close calls Close on the FallbackClient and all other connected clients
func (c *Cluster) Close() {
	if c.FallbackClient != nil {
		c.FallbackClient.Close()
	}
	for i := range c.clients {
		c.clients[i].Close()
	}
}
