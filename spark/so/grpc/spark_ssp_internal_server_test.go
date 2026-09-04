package grpc

import (
	"context"
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
