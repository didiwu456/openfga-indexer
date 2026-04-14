// Package server wires the gRPC service implementation and grpc-gateway HTTP mux.
package server

import (
	"context"
	"net"
	"net/http"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/yourorg/openfga-indexer/gateway"
	leopardv1 "github.com/yourorg/openfga-indexer/proto/leopard/v1"
)

// IndexerServer implements leopardv1.IndexerServiceServer.
type IndexerServer struct {
	leopardv1.UnimplementedIndexerServiceServer
	gw *gateway.Gateway
}

// NewIndexerServer creates an IndexerServer backed by the given Gateway.
func NewIndexerServer(gw *gateway.Gateway) *IndexerServer {
	return &IndexerServer{gw: gw}
}

func (s *IndexerServer) Check(ctx context.Context, req *leopardv1.CheckRequest) (*leopardv1.CheckResponse, error) {
	allowed, source, err := s.gw.Check(ctx, req.User, req.Relation, req.Object)
	if err != nil {
		return nil, err
	}
	return &leopardv1.CheckResponse{Allowed: allowed, Source: source}, nil
}

func (s *IndexerServer) ListObjects(ctx context.Context, req *leopardv1.ListObjectsRequest) (*leopardv1.ListObjectsResponse, error) {
	objs, source, err := s.gw.ListObjects(ctx, req.User, req.Relation, req.ObjectType)
	if err != nil {
		return nil, err
	}
	return &leopardv1.ListObjectsResponse{Objects: objs, Source: source}, nil
}

// RunGRPC starts the gRPC listener on addr.
func RunGRPC(ctx context.Context, addr string, svc *IndexerServer) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	grpcSrv := grpc.NewServer()
	leopardv1.RegisterIndexerServiceServer(grpcSrv, svc)
	hs := health.NewServer()
	hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(grpcSrv, hs)
	reflection.Register(grpcSrv)

	go func() {
		<-ctx.Done()
		grpcSrv.GracefulStop()
	}()
	return grpcSrv.Serve(lis)
}

// RunHTTP starts the grpc-gateway HTTP mux on addr, proxying to grpcAddr.
func RunHTTP(ctx context.Context, addr, grpcAddr string) error {
	mux := runtime.NewServeMux()
	if err := leopardv1.RegisterIndexerServiceHandlerFromEndpoint(
		ctx, mux, grpcAddr,
		[]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	); err != nil {
		return err
	}
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() { <-ctx.Done(); srv.Shutdown(context.Background()) }() //nolint:errcheck
	return srv.ListenAndServe()
}
