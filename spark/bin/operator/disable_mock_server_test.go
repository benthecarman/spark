package main

import (
	"testing"
	"time"

	"github.com/lightsparkdev/spark/common/keys"
	pbmock "github.com/lightsparkdev/spark/proto/mock"
	pbspark "github.com/lightsparkdev/spark/proto/spark"
	"github.com/lightsparkdev/spark/so"
	"github.com/lightsparkdev/spark/so/authninternal"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func TestDisableMockServer(t *testing.T) {
	for _, test := range []struct {
		name    string
		local   bool
		disable bool
		want    bool
	}{
		{"local default", true, false, true},
		{"local disabled", true, true, false},
		{"production", false, false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := grpc.NewServer()
			t.Cleanup(server.Stop)
			key := keys.GeneratePrivateKey()
			config := &so.Config{IdentityPrivateKey: key}
			verifier, err := authninternal.NewSessionTokenCreatorVerifier(key, nil)
			require.NoError(t, err)
			require.NoError(t, RegisterPublicGrpcServers(server, &args{
				RunningLocally:    test.local,
				DisableMockServer: test.disable,
				ChallengeTimeout:  time.Minute,
				SessionDuration:   15 * time.Minute,
			}, config, zap.NewNop(), nil, nil, verifier, nil, nil))
			services := server.GetServiceInfo()
			_, hasMock := services[pbmock.MockService_ServiceDesc.ServiceName]
			require.Equal(t, test.want, hasMock)
			require.Contains(t, services, pbspark.SparkService_ServiceDesc.ServiceName)
		})
	}
}
