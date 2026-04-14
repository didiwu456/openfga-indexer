// Command indexer-server runs the Leopard Indexer in master or replica mode.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/yourorg/openfga-indexer/admin"
	fgaclient "github.com/yourorg/openfga-indexer/client"
	"github.com/yourorg/openfga-indexer/gateway"
	"github.com/yourorg/openfga-indexer/manager"
	"github.com/yourorg/openfga-indexer/metrics"
	leopardmigrate "github.com/yourorg/openfga-indexer/migrate"
	leopardredis "github.com/yourorg/openfga-indexer/redis"
	"github.com/yourorg/openfga-indexer/server"
	leopardstorage "github.com/yourorg/openfga-indexer/storage"
	"github.com/yourorg/openfga-indexer/stringtable"
)

func main() {
	role      := flag.String("role", "replica", "master or replica")
	clusterID := flag.String("cluster", "", "unique cluster ID (required)")
	fgaURL    := flag.String("fga-url", "http://localhost:8080", "OpenFGA API URL")
	storeID   := flag.String("store", "", "FGA store ID (required)")
	dbDriver  := flag.String("db-driver", "postgres", "postgres or mysql")
	dbDSN     := flag.String("db-dsn", "", "database DSN (required)")
	redisAddr := flag.String("redis", "localhost:6379", "Redis address")
	s3Bucket  := flag.String("s3-bucket", "", "S3 bucket for shards (unused placeholder)")
	grpcAddr  := flag.String("grpc-addr", ":50051", "gRPC listen address")
	httpAddr  := flag.String("http-addr", ":8090", "HTTP/JSON listen address")
	adminAddr := flag.String("admin-addr", ":9090", "admin HTTP listen address")
	otlp      := flag.String("otlp", "", "OTLP endpoint (empty = disabled)")
	promAddr  := flag.String("prom-addr", ":9091", "Prometheus metrics address")
	flag.Parse()

	if *clusterID == "" || *storeID == "" || *dbDSN == "" {
		fmt.Fprintln(os.Stderr, "error: -cluster, -store, and -db-dsn are required")
		os.Exit(1)
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	// Run database migrations.
	if err := leopardmigrate.Run(*dbDSN, *dbDriver); err != nil {
		log.Error("migration failed", "err", err)
		os.Exit(1)
	}

	// Init observability.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	_, shutdownMetrics, err := metrics.Init(ctx, metrics.Config{
		ServiceName:    "leopard-indexer",
		OTLPEndpoint:   *otlp,
		PrometheusAddr: *promAddr,
	})
	if err != nil {
		log.Error("metrics init failed", "err", err)
		os.Exit(1)
	}
	defer shutdownMetrics(ctx) //nolint:errcheck

	// Build adapters.
	fga, err := fgaclient.New(fgaclient.Config{APIURL: *fgaURL, StoreID: *storeID})
	if err != nil {
		log.Error("FGA client failed", "err", err)
		os.Exit(1)
	}

	rstore := leopardredis.New(leopardredis.Config{Addr: *redisAddr})
	_ = s3Bucket
	stor := leopardstorage.NewMemoryStore() // replace with s3 when s3Bucket != ""

	stbl, db, err := stringtable.Open(*dbDriver, *dbDSN)
	if err != nil {
		log.Error("stringtable failed", "err", err)
		os.Exit(1)
	}
	defer db.Close() //nolint:errcheck

	// StoreManager.
	mgr := manager.New(manager.Config{
		FGA: fga, Redis: rstore, Storage: stor, StrTable: stbl,
	})
	if err := mgr.RegisterStore(ctx, *storeID, []string{"group"}); err != nil {
		log.Error("RegisterStore failed", "err", err)
		os.Exit(1)
	}

	// Check Gateway.
	idx, _ := mgr.Index(*storeID)
	gw := gateway.New(gateway.Config{
		StoreID:      *storeID,
		IndexedTypes: []string{"group"},
	}, idx, rstore, stor, stbl, fga)

	// Admin handler.
	adminHandler := admin.NewHandler(admin.Config{
		ClusterID: *clusterID,
		Role:      *role,
	}, rstore, mgr)
	adminSrv := &http.Server{Addr: *adminAddr, Handler: adminHandler}
	go adminSrv.ListenAndServe() //nolint:errcheck

	// gRPC + HTTP servers.
	svc := server.NewIndexerServer(gw)
	go func() {
		if err := server.RunGRPC(ctx, *grpcAddr, svc); err != nil {
			log.Error("gRPC server failed", "err", err)
		}
	}()
	go func() {
		if err := server.RunHTTP(ctx, *httpAddr, *grpcAddr); err != nil {
			log.Error("HTTP server failed", "err", err)
		}
	}()

	log.Info("indexer-server started", "role", *role, "cluster", *clusterID, "store", *storeID)
	<-ctx.Done()
	log.Info("shutdown complete")
}
