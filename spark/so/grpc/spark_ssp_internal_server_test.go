package grpc

import (
	"context"
	"github.com/lightsparkdev/spark/common/keys"
	"github.com/lightsparkdev/spark/so"
	"github.com/lightsparkdev/spark/so/authn"
	"github.com/lightsparkdev/spark/so/authz"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strings"
	"testing"

	pbspark "github.com/lightsparkdev/spark/proto/spark"
	"github.com/stretchr/testify/require"
)

type recordingSSPTreeCreationHandler struct {
	prepareRequest *pbspark.PrepareTreeAddressRequest
	createRequest  *pbspark.CreateTreeRequest
}

func (h *recordingSSPTreeCreationHandler) PrepareTreeAddress(_ context.Context, req *pbspark.PrepareTreeAddressRequest) (*pbspark.PrepareTreeAddressResponse, error) {
	h.prepareRequest = req
	return &pbspark.PrepareTreeAddressResponse{}, nil
}

func (h *recordingSSPTreeCreationHandler) CreateTreeV2(_ context.Context, req *pbspark.CreateTreeRequest) (*pbspark.CreateTreeResponse, error) {
	h.createRequest = req
	return &pbspark.CreateTreeResponse{}, nil
}

func TestSparkSspInternalServerDelegatesToTreeCreationV2(t *testing.T) {
	handler := &recordingSSPTreeCreationHandler{}
	server := &SparkSspInternalServer{treeCreationHandler: handler}
	prepareRequest := &pbspark.PrepareTreeAddressRequest{}
	createRequest := &pbspark.CreateTreeRequest{}

	prepareResponse, err := server.PrepareTreeAddress(t.Context(), prepareRequest)
	require.NoError(t, err)
	require.NotNil(t, prepareResponse)
	require.Same(t, prepareRequest, handler.prepareRequest)

	createResponse, err := server.CreateTree(t.Context(), createRequest)
	require.NoError(t, err)
	require.NotNil(t, createResponse)
	require.Same(t, createRequest, handler.createRequest)
}

func TestPrivateWalletReadsRequireAllowedSSP(t *testing.T) {
	server := NewSparkSspInternalServer(&so.Config{ServiceAuthz: so.ServiceAuthzConfig{Mode: authz.ModeEnforce}})
	key := keys.GeneratePrivateKey().Public()
	ctx := authn.InjectSessionForTests(t.Context(), key, 9999999999)
	t.Setenv("SSP_INTERNAL_ALLOWED_IDENTITIES", "")
	require.Equal(t, codes.Unauthenticated, status.Code(server.requireSSP(t.Context())))
	require.Equal(t, codes.PermissionDenied, status.Code(server.requireSSP(ctx)))
	_, err := server.QueryNodes(ctx, &pbspark.QueryNodesRequest{})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	_, err = server.QueryStaticDepositAddresses(ctx, &pbspark.QueryStaticDepositAddressesRequest{})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	_, err = server.ReserveInstantDeposit(ctx, nil)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	_, err = server.RecoverInstantDeposit(ctx, nil)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	t.Setenv("SSP_INTERNAL_ALLOWED_IDENTITIES", "other, "+strings.ToUpper(key.ToHex())+" ")
	require.NoError(t, server.requireSSP(ctx))
	other := authn.InjectSessionForTests(t.Context(), keys.GeneratePrivateKey().Public(), 9999999999)
	require.Equal(t, codes.PermissionDenied, status.Code(server.requireSSP(other)))
}
