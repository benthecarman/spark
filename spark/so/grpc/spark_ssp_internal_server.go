package grpc

import (
	"context"
	"github.com/lightsparkdev/spark/so/authn"
	"github.com/lightsparkdev/spark/so/authz"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"os"
	"strings"

	pbspark "github.com/lightsparkdev/spark/proto/spark"
	pbssp "github.com/lightsparkdev/spark/proto/spark_ssp_internal"
	"github.com/lightsparkdev/spark/so"
	"github.com/lightsparkdev/spark/so/handler"
)

// SparkSspInternalServer handles authenticated SSP requests that must not be exposed on the wallet-facing listener.
type SparkSspInternalServer struct {
	pbssp.UnimplementedSparkSspInternalServiceServer
	treeCreationHandler sspTreeCreationHandler
	queryHandler        *handler.TreeQueryHandler
	config              *so.Config
}

// NewSparkSspInternalServer creates an SSP-internal server backed by the tree-creation flow.
func NewSparkSspInternalServer(config *so.Config) *SparkSspInternalServer {
	return &SparkSspInternalServer{treeCreationHandler: handler.NewTreeCreationHandler(config), queryHandler: handler.NewTreeQueryHandler(config), config: config}
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

// QueryNodes is restricted by the private listener and SSP session policy.
// Public wallet queries retain their wallet privacy checks.
func (s *SparkSspInternalServer) QueryNodes(ctx context.Context, req *pbspark.QueryNodesRequest) (*pbspark.QueryNodesResponse, error) {
	if err := s.requireSSP(ctx); err != nil {
		return nil, err
	}
	return s.queryHandler.QueryNodes(ctx, req, true)
}

// A network location and a wallet session alone do not grant private wallet
// read access. Operators explicitly list the SSP identities they trust.
func (s *SparkSspInternalServer) requireSSP(ctx context.Context) error {
	if s.config.ServiceAuthz.Mode == authz.ModeDisabled {
		return nil
	}
	session, err := authn.GetSessionFromContext(ctx)
	if err != nil {
		return status.Error(codes.Unauthenticated, "SSP session required")
	}
	for _, allowed := range strings.Split(os.Getenv("SSP_INTERNAL_ALLOWED_IDENTITIES"), ",") {
		if strings.EqualFold(strings.TrimSpace(allowed), session.IdentityPublicKey().ToHex()) {
			return nil
		}
	}
	return status.Error(codes.PermissionDenied, "identity is not an authorized SSP")
}

func (s *SparkSspInternalServer) QueryStaticDepositAddresses(ctx context.Context, req *pbspark.QueryStaticDepositAddressesRequest) (*pbspark.QueryStaticDepositAddressesResponse, error) {
	if err := s.requireSSP(ctx); err != nil {
		return nil, err
	}
	return s.queryHandler.QueryStaticDepositAddresses(ctx, req, true)
}
func (s *SparkSspInternalServer) InitiateStaticDepositSwap(ctx context.Context, req *pbssp.StaticDepositSwapRequest) (*pbssp.StaticDepositSwapResponse, error) {
	if err := s.requireSSP(ctx); err != nil {
		return nil, err
	}
	return handler.NewStaticDepositHandler(s.config).InitiateSSPStaticDepositSwap(ctx, req)
}
