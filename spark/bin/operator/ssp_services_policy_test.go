package main

import (
	"testing"
	"time"

	"github.com/lightsparkdev/spark/common/keys"
	pbauthn "github.com/lightsparkdev/spark/proto/spark_authn"
	pbssp "github.com/lightsparkdev/spark/proto/spark_ssp_internal"
	"github.com/lightsparkdev/spark/so"
	"github.com/lightsparkdev/spark/so/authninternal"
	"github.com/lightsparkdev/spark/so/rpcpolicy"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestSSPListenerRegistrationAndPolicy(t *testing.T) {
	server := grpc.NewServer()
	identityPrivateKey := keys.GeneratePrivateKey()
	config := &so.Config{IdentityPrivateKey: identityPrivateKey}
	verifier, err := authninternal.NewSessionTokenCreatorVerifier(identityPrivateKey, nil)
	require.NoError(t, err)
	require.NoError(t, RegisterSSPGrpcServers(server, &args{
		ChallengeTimeout: time.Minute,
		SessionDuration:  15 * time.Minute,
	}, config, verifier))

	services := server.GetServiceInfo()
	require.Len(t, services, 2)
	_, ok := services[pbauthn.SparkAuthnService_ServiceDesc.ServiceName]
	require.True(t, ok)
	service, ok := services[pbssp.SparkSspInternalService_ServiceDesc.ServiceName]
	require.True(t, ok)
	require.Len(t, service.Methods, 2)

	for _, method := range []string{
		pbssp.SparkSspInternalService_PrepareTreeAddress_FullMethodName,
		pbssp.SparkSspInternalService_CreateTree_FullMethodName,
	} {
		policy, ok := rpcpolicy.LookUp(method)
		require.True(t, ok)
		require.Equal(t, rpcpolicy.AuthSession, policy.AuthMode)
		require.True(t, policy.InternalOnly)
	}
	for _, method := range []string{
		pbauthn.SparkAuthnService_GetChallenge_FullMethodName,
		pbauthn.SparkAuthnService_VerifyChallenge_FullMethodName,
	} {
		policy, ok := rpcpolicy.LookUp(method)
		require.True(t, ok)
		require.Equal(t, rpcpolicy.AuthAnonymous, policy.AuthMode)
		require.False(t, policy.InternalOnly)
	}
}
