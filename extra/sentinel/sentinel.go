// The sentinel package provides a convenient interface with a redis sentinel
// which will automatically handle pooling connections and automatic failover.
//
// Here's an example of creating a sentinel client and then using it to perform
// some commands
//
//	func example() error {
//		// If there exists sentinel masters "bucket0" and "bucket1", and we want out
//		// client to create pools for both:
//		client, err := sentinel.NewClient("tcp", "localhost:6379", 100, "bucket0", "bucket1")
//		if err != nil {
//			return err
//		}
//
//		if err := exampleCmd(client); err != nil {
//			return err
//		}
//
//		return nil
//	}
//
//	func exampleCmd(client *sentinel.Client) error {
//		conn, redisErr := client.GetMaster("bucket0")
//		if redisErr != nil {
//			return redisErr
//		}
//		// We use CarefullyPutMaster to conditionally put the connection back in the
//		// pool depending on the last error seen
//		defer client.CarefullyPutMaster("bucket0", conn, &redisErr)
//
//		var i int
//		if i, redisErr = conn.Cmd("GET", "foo").Int(); redisErr != nil {
//			return redisErr
//		}
//
//		if redisErr = conn.Cmd("SET", "foo", i+1); redisErr != nil {
//			return redisErr
//		}
//
//		return nil
//	}
//
// This package only gaurantees that when GetMaster is called the returned
// connection will be a connection to the master as of the moment that method is
// called. It is still possible that there is a failover as that connection is
// being used by the application. The Readonly() method on CmdError will be
// helpful if you want to gracefully handle this case.
//
// As a final note, a Client can be interacted with from multiple routines at
// once safely, except for the Close method. To safely Close, ensure that only
// one routine ever makes the call and that once the call is made no other
// methods are ever called by any routines.
package sentinel

import (
	"errors"
	"fmt"
	"github.com/rightscale/radix/redis"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rightscale/radix/extra/pool"
	"github.com/rightscale/radix/extra/pubsub"
	"github.com/rightscale/radix/logging"
)

const (
	acceptableRequestDuration = time.Duration(1 * time.Second)
)

// An error wrapper returned by operations in this package. It implements the
// error interface and can therefore be passed around as a normal error.
type ClientError struct {
	err error

	// If this is true the error is due to a problem with the sentinel
	// connection, either it being closed or otherwise unavailable. If false the
	// error is due to some other circumstances. This is useful if you want to
	// implement some kind of reconnecting to sentinel on an error.
	SentinelErr bool
}

func (ce *ClientError) Error() string {
	return ce.err.Error()
}

type getReqRet struct {
	conn *redis.Client
	err  *ClientError
}

type getReq struct {
	reqId string

	name  string
	retCh chan *getReqRet
}

type putReq struct {
	reqId string

	name string
	conn *redis.Client
}

type switchMaster struct {
	name string
	addr string
}

type Client struct {
	logger *logging.LoggerWithPrefix

	initPoolSize int
	poolSize     int
	masterPools  map[string]*pool.Pool
	subClient    *pubsub.SubClient

	getCh   chan *getReq
	putCh   chan *putReq
	closeCh chan struct{}

	alwaysErr      *ClientError
	alwaysErrCh    chan *ClientError
	switchMasterCh chan *switchMaster

	reqId uint64
}

// Creates a sentinel client. Connects to the given sentinel instance, pulls the
// information for the masters of the given names, and creates an intial pool of
// connections for each master. The client will automatically replace the pool
// for any master should sentinel decide to fail the master over. The returned
// error is a *ClientError.
func NewClient(
	network, address string, poolSize int, names ...string,
) (
	*Client, error,
) {
	return NewClientWithLogger(logging.NewNilLogger(), network, address, poolSize, poolSize, names...)
}

func NewClientWithLogger(
	logger logging.SimpleLogger, network, address string, initPoolSize, poolSize int, names ...string,
) (
	*Client, error,
) {
	prefixedLogger := logging.NewLoggerWithPrefix("[SC]", logger)
	initLogger := prefixedLogger.WithAnotherPrefix("[init]")

	initLogger.Infof("Setting up with network:%s, addr:%s, masterNames:%v, "+
		"initPoolSize:%d, softMaxPoolSize:%d",
		network, address, names, initPoolSize, poolSize)
	//
	// Connect to sentinel
	// We use this to fetch initial details about masters before we upgrade it
	// to a pubsub client
	client, err := redis.Dial(network, address)
	if err != nil {
		initLogger.Infof("Connecting to sentinel with addr='%s' errored: %v", address, err)
		return nil, &ClientError{err: err}
	}
	initLogger.Infof("Connected to Sentinel with addr: '%s'", address)

	//
	// Setup connection pools for all redis masters
	masterPools := map[string]*pool.Pool{}
	for _, name := range names {
		initLogger.Infof("Initializing connection pool for redis master '%s'", name)
		r := client.Cmd("SENTINEL", "MASTER", name)
		l, err := r.List()
		if err != nil {
			initLogger.Infof("Sentinel master command for redis master '%s' errored: %v", address, err)
			return nil, &ClientError{err: err, SentinelErr: true}
		}
		addr := l[3] + ":" + l[5]

		initLogger.Infof("Setting up Redis Connection Pool with addr: '%s'", addr)
		pool, err := pool.NewPool("tcp", addr, initPoolSize, poolSize)
		if err != nil {
			initLogger.Infof("Init redis connection pool for redis master '%s' errored: %v", addr, err)
			return nil, &ClientError{err: err}
		}
		masterPools[name] = pool
	}

	//
	// Upgrade sentinel client connection to pubSub Client
	initLogger.Infof("Subscribing to +switch-master events")
	subClient := pubsub.NewSubClient(client)
	r := subClient.Subscribe("+switch-master")
	if r.Err != nil {
		initLogger.Infof("Subscribe call to +switch-master errored: %v", err)
		return nil, &ClientError{err: r.Err, SentinelErr: true}
	}

	initLogger.Infof("Subscribed to +switch-master events")
	c := &Client{
		initPoolSize:   initPoolSize,
		poolSize:       poolSize,
		masterPools:    masterPools,
		subClient:      subClient,
		getCh:          make(chan *getReq),
		putCh:          make(chan *putReq),
		closeCh:        make(chan struct{}),
		alwaysErrCh:    make(chan *ClientError),
		switchMasterCh: make(chan *switchMaster),
		reqId:          newReqId(),
		logger:         prefixedLogger,
	}

	go c.subSpin()
	go c.spin()
	initLogger.Infof("Initialization completed")
	return c, nil
}

func (c *Client) subSpin() {
	logger := c.logger.WithAnotherPrefix("[Sub]")
	for {
		r := c.subClient.Receive()
		if r.Timeout() {
			logger.Infof("Receive() timed out")
			continue
		}
		if r.Err != nil {
			select {
			case c.alwaysErrCh <- &ClientError{err: r.Err, SentinelErr: true}:
			case <-c.closeCh:
			}
			logger.Infof("Receive() returned error %v ", r.Err)
			return
		}
		sMsg := strings.Split(r.Message, " ")
		name := sMsg[0]
		newAddr := sMsg[3] + ":" + sMsg[4]
		logger.Infof("Receive() returned message %s ", r.Message)

		select {
		case c.switchMasterCh <- &switchMaster{name, newAddr}:
			logger.Infof("Sent message to 'switchMasterCh' channel.")
		case <-c.closeCh:
			logger.Infof("Handling 'closeCh' message.")
			return
		}
	}
}

func (c *Client) spin() {
	for {
		select {
		case req := <-c.getCh:
			startTime := time.Now()
			logger := c.loggerWithGetRequestPrefix(req.reqId)
			logger.Debugf("Received request")

			if c.alwaysErr != nil {
				logger.Infof("Failing request due to alwaysErr being set")
				req.retCh <- &getReqRet{nil, c.alwaysErr}
				continue
			}
			pool, ok := c.masterPools[req.name]
			if !ok {
				logger.Infof("Failing request due to unknown master pool '%s'", req.name)

				err := errors.New("unknown name: " + req.name)
				req.retCh <- &getReqRet{nil, &ClientError{err: err}}
				continue
			}

			conn, err := pool.Get()
			if err != nil {
				logger.Infof("Failing request due to pool.Get() error: %s", err.Error())

				req.retCh <- &getReqRet{nil, &ClientError{err: err}}
				continue
			}

			req.retCh <- &getReqRet{conn, nil}

			logger.Debugf("Completed request in %s.", time.Since(startTime))

		case req := <-c.putCh:
			startTime := time.Now()
			logger := c.loggerWithPutRequestPrefix(req.reqId)

			logger.Debugf("Received request")

			if pool, ok := c.masterPools[req.name]; ok {
				logger.Debugf("Returning connection to '%s' pool.", req.name)
				pool.Put(req.conn)
			}
			logger.Debugf("Completed request in %s.", time.Since(startTime))

		case err := <-c.alwaysErrCh:
			logger := c.logger.WithAnotherPrefix("[AlwaysError]")
			logger.Infof("Unrecoverable error encountered: '%s'", err.Error())

			c.alwaysErr = err

		case sm := <-c.switchMasterCh:
			logger := c.logger.WithAnotherPrefix("[SwitchMaster]")

			if p, ok := c.masterPools[sm.name]; ok {
				logger.Infof("Starting master switch for '%s' master to addr: '%s'",
					sm.name, sm.addr)

				logger.Infof("Emptying current master pool '%s'", sm.name)
				p.Empty()

				logger.Infof("Initializing new master pool for '%s'", sm.name)
				p = pool.NewOrEmptyPool("tcp", sm.addr, c.initPoolSize, c.poolSize)

				c.masterPools[sm.name] = p
				logger.Infof("Completed master switch for '%s' master with addr: '%s'",
					sm.name, sm.addr)
			} else {
				logger.Infof(
					"Received master switch request for uninitialized master pool '%s' with addr='%s'",
					sm.name, sm.addr)
			}

		case <-c.closeCh:
			logger := c.logger.WithAnotherPrefix("[Close]")

			logger.Infof("Closing...")

			for name := range c.masterPools {
				logger.Infof("Emptying master pool %s", name)
				c.masterPools[name].Empty()
			}

			logger.Infof("Closing subclient")
			c.subClient.Client.Close()

			logger.Infof("Closing request & response channels")
			close(c.getCh)
			close(c.putCh)
			logger.Infof("Closed.")
			return
		}
	}
}

// Retrieves a connection for the master of the given name. If sentinel has
// become unreachable this will always return an error. Close should be called
// in that case. The returned error is a *ClientError.
func (c *Client) GetMaster(name string) (*redis.Client, error) {
	req := &getReq{c.incReqId(), name, make(chan *getReqRet)}
	startTime := time.Now()

	logger := c.loggerWithGetRequestPrefix(req.reqId)
	logger.Debugf("Received GetMaster request")

	c.getCh <- req
	ret := <-req.retCh
	if ret.err != nil {
		logger.Warnf("GetMaster request failed with error: %s", ret.err.Error())
		return nil, ret.err
	}

	c.logRequestTiming(logger, startTime, "GetMaster")
	return ret.conn, nil
}

// Return a connection for a master of a given name. As with the pool package,
// do not return a connection which is having connectivity issues, or which is
// otherwise unable to perform requests.
func (c *Client) PutMaster(name string, client *redis.Client) {
	req := &putReq{c.incReqId(), name, client}
	startTime := time.Now()

	logger := c.loggerWithPutRequestPrefix(req.reqId)
	logger.Debugf("PutMaster request received")

	c.putCh <- req

	c.logRequestTiming(logger, startTime, "PutMaster")
}

// A useful helper method, analagous to the pool package's CarefullyPut method.
// Since we don't want to Put a connection which is having connectivity
// issues, this can be defered inside a function to make sure we only put back a
// connection when we should. It should be used like the following:
//
//	func doSomeThings(c *Client) error {
//		conn, redisErr := c.GetMaster("bucket0")
//		if redisErr != nil {
//			return redisErr
//		}
//		defer c.CarefullyPutMaster("bucket0", conn, &redisErr)
//
//		var i int
//		i, redisErr = conn.Cmd("GET", "foo").Int()
//		if redisErr != nil {
//			return redisErr
//		}
//
//		redisErr = conn.Cmd("SET", "foo", i * 3).Err
//		return redisErr
//	}
func (c *Client) CarefullyPutMaster(
	name string, client *redis.Client, potentialErr *error,
) {
	if potentialErr != nil && *potentialErr != nil {
		// If the client sent back that it's READONLY then we don't want to keep
		// this connection around. Otherwise, we don't care about command errors
		if cerr, ok := (*potentialErr).(*redis.CmdError); !ok || cerr.Readonly() {
			logger := c.logger.WithAnotherPrefix("[CarefullyPutMaster]")
			logger.Infof("Closing readonly connection...")
			client.Close()
			logger.Infof("Readonly connection closed.")
			return
		}
	}
	c.PutMaster(name, client)
}

// Closes all connection pools as well as the connection to sentinel.
func (c *Client) Close() {
	c.logger.Infof("Closing all connection pools & sentinel connection...")
	close(c.closeCh)
}
func (c *Client) logRequestTiming(logger logging.SimpleLogger, startTime time.Time, requestType string) {
	timeTaken := time.Since(startTime)
	msg := fmt.Sprintf("%s request completed in %s", requestType, timeTaken)

	if timeTaken > acceptableRequestDuration {
		logger.Warnf(msg)
	} else {
		logger.Debugf(msg)
	}
}

func (c *Client) loggerWithGetRequestPrefix(reqId string) *logging.LoggerWithPrefix {
	return c.loggerWithRequestPrefix("get", reqId)
}

func (c *Client) loggerWithPutRequestPrefix(reqId string) *logging.LoggerWithPrefix {
	return c.loggerWithRequestPrefix("put", reqId)
}

func (c *Client) loggerWithRequestPrefix(requestType, reqId string) *logging.LoggerWithPrefix {
	return c.logger.WithAnotherPrefix(fmt.Sprintf("[%sReq-%s]", requestType, reqId))
}

func newReqId() uint64 {
	r := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
	return uint64((r.Int63n(1<<14) + 1) << 8)
}

// generateID generates the request id by using the initial start point and
// incrementing it by one atomatically
func (c *Client) incReqId() string {
	// atomic.AddUint64 handles overflow
	id := atomic.AddUint64(&c.reqId, 1)
	return fmt.Sprintf("%x", id)
}
