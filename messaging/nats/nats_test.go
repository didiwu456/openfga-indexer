//go:build integration

package nats_test

import (
	"os"
	"testing"

	"github.com/yourorg/openfga-indexer/messaging"
	leopardnats "github.com/yourorg/openfga-indexer/messaging/nats"
)

func TestNatsBus_Contract(t *testing.T) {
	url := os.Getenv("TEST_NATS_URL")
	if url == "" {
		t.Skip("TEST_NATS_URL not set")
	}
	bus, err := leopardnats.New(leopardnats.Config{
		URL:        url,
		ConsumerID: "test-consumer",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer bus.Close()
	messaging.RunBusContract(t, bus)
}
