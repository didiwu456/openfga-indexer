// Package metrics initialises OpenTelemetry (traces + metrics) and exposes
// a Prometheus /metrics endpoint. All named instruments are defined here so
// other packages import only this package to record measurements.
package metrics

import (
	"context"
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	promexporter "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// Config holds observability settings.
type Config struct {
	ServiceName    string
	OTLPEndpoint   string // e.g. "otel-collector:4317"; empty = traces disabled
	PrometheusAddr string // e.g. ":9091"
}

// Instruments holds all named metric instruments.
type Instruments struct {
	CheckTotal        metric.Int64Counter
	CheckDuration     metric.Float64Histogram
	RebuildDuration   metric.Float64Histogram
	RebuildTupleCount metric.Int64Gauge
	ShardAgeSec       metric.Float64Gauge
	WatcherLag        metric.Int64Gauge
	CircuitBreaker    metric.Int64Gauge
	StringTableSize   metric.Int64Gauge
}

// Init sets up OTel traces and metrics. Returns Instruments and a shutdown func.
func Init(ctx context.Context, cfg Config) (*Instruments, func(context.Context) error, error) {
	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName(cfg.ServiceName)),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("metrics.Init: resource: %w", err)
	}

	// ── Traces ────────────────────────────────────────────────────────────────
	var tp *sdktrace.TracerProvider
	if cfg.OTLPEndpoint != "" {
		exp, err := otlptracegrpc.New(ctx,
			otlptracegrpc.WithEndpoint(cfg.OTLPEndpoint),
			otlptracegrpc.WithInsecure(),
		)
		if err != nil {
			return nil, nil, fmt.Errorf("metrics.Init: trace exporter: %w", err)
		}
		tp = sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(exp),
			sdktrace.WithResource(res),
		)
	} else {
		tp = sdktrace.NewTracerProvider(sdktrace.WithResource(res))
	}
	otel.SetTracerProvider(tp)

	// ── Metrics (Prometheus bridge) ───────────────────────────────────────────
	promExp, err := promexporter.New()
	if err != nil {
		return nil, nil, fmt.Errorf("metrics.Init: prometheus exporter: %w", err)
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(promExp),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)

	meter := mp.Meter("leopard")

	inst := &Instruments{}
	inst.CheckTotal, _ = meter.Int64Counter("leopard_check_total",
		metric.WithDescription("Total Check calls by store, source, and result"))
	inst.CheckDuration, _ = meter.Float64Histogram("leopard_check_duration_seconds",
		metric.WithDescription("Check latency in seconds"))
	inst.RebuildDuration, _ = meter.Float64Histogram("leopard_rebuild_duration_seconds",
		metric.WithDescription("Offline Pipeline rebuild duration in seconds"))
	inst.RebuildTupleCount, _ = meter.Int64Gauge("leopard_rebuild_tuple_count",
		metric.WithDescription("Number of tuples processed in last rebuild per store"))
	inst.ShardAgeSec, _ = meter.Float64Gauge("leopard_shard_age_seconds",
		metric.WithDescription("Age of the active shard in seconds"))
	inst.WatcherLag, _ = meter.Int64Gauge("leopard_watcher_lag_changes",
		metric.WithDescription("Estimated lag in ReadChanges events for the compute tier"))
	inst.CircuitBreaker, _ = meter.Int64Gauge("leopard_circuit_breaker_state",
		metric.WithDescription("Circuit breaker state: 0=closed, 1=open, 2=half-open"))
	inst.StringTableSize, _ = meter.Int64Gauge("leopard_string_table_size",
		metric.WithDescription("Number of entries in the string table per store"))

	// Start Prometheus HTTP server.
	if cfg.PrometheusAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		srv := &http.Server{Addr: cfg.PrometheusAddr, Handler: mux}
		go srv.ListenAndServe() //nolint:errcheck
	}

	shutdown := func(ctx context.Context) error {
		_ = tp.Shutdown(ctx)
		_ = mp.Shutdown(ctx)
		return nil
	}
	return inst, shutdown, nil
}
