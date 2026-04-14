// Command write-proxy runs the Write Proxy service.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	fgaclient "github.com/yourorg/openfga-indexer/client"
	"github.com/yourorg/openfga-indexer/messaging"
	"github.com/yourorg/openfga-indexer/proxy"
)

func main() {
	fgaURL  := flag.String("fga-url", "http://localhost:8080", "OpenFGA API URL")
	storeID := flag.String("store", "", "FGA store ID (required)")
	flag.String("grpc-addr", ":50052", "gRPC listen address (reserved for future use)")
	flag.Parse()

	if *storeID == "" {
		fmt.Fprintln(os.Stderr, "error: -store is required")
		os.Exit(1)
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	fga, err := fgaclient.New(fgaclient.Config{APIURL: *fgaURL, StoreID: *storeID})
	if err != nil {
		log.Error("FGA client failed", "err", err)
		os.Exit(1)
	}

	// Wire real MessageBus here; nop for standalone start.
	_ = proxy.New(fga, &nopBus{}, proxy.Options{StoreID: *storeID})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Info("write-proxy started", "store", *storeID)
	<-ctx.Done()
	log.Info("shutdown complete")
}

type nopBus struct{}

func (n *nopBus) PublishRebuild(_ context.Context, _ messaging.RebuildEvent) error { return nil }
