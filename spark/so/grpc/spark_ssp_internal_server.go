package grpc

import (
	"context"

	pbspark "github.com/lightsparkdev/spark/proto/spark"
	pbssp "github.com/lightsparkdev/spark/proto/spark_ssp_internal"
	"github.com/lightsparkdev/spark/so"
	"github.com/lightsparkdev/spark/so/handler"
)

// SparkSspInternalServer handles authenticated SSP requests that must not be exposed on the wallet-facing listener.
type SparkSspInternalServer struct {
	pbssp.UnimplementedSparkSspInternalServiceServer
	treeCreationHandler sspTreeCreationHandler
}

// NewSparkSspInternalServer creates an SSP-internal server backed by the tree-creation flow.
func NewSparkSspInternalServer(config *so.Config) *SparkSspInternalServer {
	return &SparkSspInternalServer{treeCreationHandler: handler.NewTreeCreationHandler(config)}
}

func (s *SparkSspInternalServer) PrepareTreeAddress(ctx context.Context, req *pbspark.PrepareTreeAddressRequest) (*pbspark.PrepareTreeAddressResponse, error) {
	return s.treeCreationHandler.PrepareTreeAddress(ctx, req)
}

// CreateTree requires the direct-exit transaction package enforced by CreateTreeV2.
func (s *SparkSspInternalServer) CreateTree(ctx context.Context, req *pbspark.CreateTreeRequest) (*pbspark.CreateTreeResponse, error) {
	return s.treeCreationHandler.CreateTreeV2(ctx, req)
}

type sspTreeCreationHandler interface {
	PrepareTreeAddress(context.Context, *pbspark.PrepareTreeAddressRequest) (*pbspark.PrepareTreeAddressResponse, error)
	CreateTreeV2(context.Context, *pbspark.CreateTreeRequest) (*pbspark.CreateTreeResponse, error)
}
