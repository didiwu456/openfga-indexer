//go:build integration

package redisstream_test

import (
	"os"
	"testing"

	"github.com/yourorg/openfga-indexer/messaging"
	"github.com/yourorg/openfga-indexer/messaging/redisstream"
)

func TestRedisStreamBus_Contract(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR not set")
	}
	bus, err := redisstream.New(redisstream.Config{
		Addr:       addr,
		ConsumerID: "test-consumer",
		GroupID:    "test-group",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer bus.Close()
	messaging.RunBusContract(t, bus)
}
