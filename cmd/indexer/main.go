// Command indexer runs the three-layer Leopard indexing system:
//
//  1. Builder  — periodically scans all FGA tuples and builds a fresh shard.
//  2. Cache    — holds the active shard; Builder rotates it in atomically.
//  3. Watcher  — tails ReadChanges and applies incremental deltas to the shard;
//               re-anchors its continuation token after each Builder rotation.
//
// Usage:
//
//	indexer -api http://localhost:8080 -store <storeID>
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yourorg/openfga-indexer/builder"
	"github.com/yourorg/openfga-indexer/cache"
	"github.com/yourorg/openfga-indexer/check"
	fgaclient "github.com/yourorg/openfga-indexer/client"
	"github.com/yourorg/openfga-indexer/watcher"
)

func main() {
	apiURL          := flag.String("api", "http://localhost:8080", "OpenFGA API URL")
	storeID         := flag.String("store", "", "OpenFGA store ID (required)")
	rebuildInterval := flag.Duration("rebuild", time.Hour, "Offline index rebuild interval")
	pollInterval    := flag.Duration("poll", 5*time.Second, "Watcher poll interval")
	flag.Parse()

	if *storeID == "" {
		fmt.Fprintln(os.Stderr, "error: -store is required")
		flag.Usage()
		os.Exit(1)
	}

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	slog.SetDefault(log)

	// ── 1. FGA client ────────────────────────────────────────────────────────
	c, err := fgaclient.New(fgaclient.Config{APIURL: *apiURL, StoreID: *storeID})
	if err != nil {
		log.Error("failed to create FGA client", "err", err)
		os.Exit(1)
	}

	// ── 2. Cache (shard store) ────────────────────────────────────────────────
	ca := cache.New()

	// ── 3. Builder (offline periodic full-scan) ───────────────────────────────
	b := builder.New(c, ca, builder.Options{
		StoreID:         *storeID,
		RebuildInterval: *rebuildInterval,
		Logger:          log,
	})

	// ── 4. Watcher (incremental ReadChanges) ──────────────────────────────────
	w := watcher.New(c, ca, watcher.Options{
		StoreID:      *storeID,
		PollInterval: *pollInterval,
		Logger:       log,
	})

	// ── 5. Checker (serves queries) ───────────────────────────────────────────
	checker := check.New(ca, c, *storeID, "")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Initial build: populate the cache before the watcher starts.
	log.Info("running initial offline build…")
	token, err := b.RunOnce(ctx)
	if err != nil {
		log.Error("initial build failed", "err", err)
		os.Exit(1)
	}
	ca.SetWatchToken(token)

	snap := ca.Snapshot()
	log.Info("initial shard ready",
		"member_to_group_entries", len(snap.MemberToGroup),
		"group_to_group_entries", len(snap.GroupToGroup),
	)

	// Example query against the warm cache.
	ok, err := checker.Check(ctx, "user:alice", "member", "group:engineering")
	if err != nil {
		log.Error("check failed", "err", err)
	} else {
		log.Info("example check",
			"user", "user:alice", "relation", "member",
			"object", "group:engineering", "allowed", ok,
		)
	}

	// Run builder and watcher concurrently.
	builderDone := make(chan error, 1)
	go func() { builderDone <- b.Run(ctx) }()

	watcherDone := make(chan error, 1)
	go func() { watcherDone <- w.Run(ctx) }()

	// Wait for shutdown.
	select {
	case err := <-builderDone:
		if err != nil {
			log.Error("builder exited with error", "err", err)
		}
	case err := <-watcherDone:
		if err != nil {
			log.Error("watcher exited with error", "err", err)
		}
	}
	log.Info("shutdown complete")
}
