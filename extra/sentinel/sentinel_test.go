package sentinel

import (
	"github.com/rightscale/radix/redis"
	"strings"
	"testing"
)

func TestSentinelConnectionError(t *testing.T) {
	redisMaster := "mymaster"

	sentinelClient := newSentinelClient(t, redisMaster)
	defer sentinelClient.Close()

	// get the pointer to redisClient in the poll so that we can
	// check its connection later
	redisClient := getRedisClient(t, sentinelClient, redisMaster)
	sentinelClient.PutMaster(redisMaster, redisClient)

	// Simulating a Sentinel connection failure
	sentinelClient.subClient.Client.Close()

	// The connection in the connection pool should be closed
	verifyConnectionClosed(t, redisClient)
}

func TestPutMaster(t *testing.T) {
	redisMaster := "mymaster"

	sentinelClient := newSentinelClient(t, redisMaster)
	defer sentinelClient.Close()

	redisClient := getRedisClient(t, sentinelClient, redisMaster)

	// Simulating a Sentinel connection failure
	sentinelClient.subClient.Client.Close()

	// Returning the redisClient to a disconnected Sentinel Client
	// should close the redisClient's connection
	sentinelClient.PutMaster(redisMaster, redisClient)
	verifyConnectionClosed(t, redisClient)
}

func newSentinelClient(t *testing.T, redisMaster string) *Client {
	sentinelClient, err := NewClient("tcp", "localhost:26379", 1, redisMaster)
	if err != nil {
		t.Fatal(err)
	}
	return sentinelClient
}

func getRedisClient(t *testing.T, sentinelClient *Client, redisMaster string) *redis.Client {
	redisClient, err := sentinelClient.GetMaster(redisMaster)
	if err != nil {
		t.Fatal(err)
	}
	return redisClient
}

func verifyConnectionClosed(t *testing.T, redisClient *redis.Client) {
	reply := redisClient.Cmd("PING")

	if reply.Err == nil {
		t.Fatal("it should have closed connections")
	} else if !strings.Contains(reply.Err.Error(), "use of closed network connection") {
		t.Fatal("it should get a connection already closed error but got error: ", reply.Err)
	}
}
