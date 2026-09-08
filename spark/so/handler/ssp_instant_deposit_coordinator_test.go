package handler

import (
	"github.com/lightsparkdev/spark/common/keys"
	pbspark "github.com/lightsparkdev/spark/proto/spark"
	pbssp "github.com/lightsparkdev/spark/proto/spark_ssp_internal"
	"github.com/lightsparkdev/spark/so/authn"
	"github.com/lightsparkdev/spark/so/authz"
	"github.com/lightsparkdev/spark/so/db"
	st "github.com/lightsparkdev/spark/so/ent/schema/schematype"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"testing"
)

func TestSSPInstantRecoveryReplayBindsOwnerTransactionAndNonce(t *testing.T) {
	ctx, _ := db.ConnectToTestPostgres(t)
	fixture := createTestClaimableInstantSwap(t, ctx, nil)
	prepared := claimPrepareOpFor(t, fixture, nil, []byte("persisted recovery transaction")).OriginalRequest
	input := &pbssp.RecoverInstantDepositRequest{OnChainUtxo: prepared.OnChainUtxo, SpendTxSigningJob: prepared.SpendTxSigningJob, TransferId: prepared.TransferId}
	// A completed replay must return saved shares without another signing round.
	result := &pbspark.SigningResult{SignatureShares: map[string][]byte{"operator": {1}}}
	checkpoint, err := proto.Marshal(&pbssp.InstantRecoveryCheckpoint{Request: input, SigningResult: result})
	require.NoError(t, err)
	_, err = fixture.swap.Update().SetSpendTxSigningResult(checkpoint).SetStatus(st.UtxoSwapStatusCompleted).Save(ctx)
	require.NoError(t, err)
	config := setUpTestConfigWithRegtestNoAuthz(t)
	config.ServiceAuthz.Mode = authz.ModeEnforce
	config.AuthzEnforced = true
	handler := NewStaticDepositHandler(config)
	owner := authn.InjectSessionForTests(ctx, fixture.swap.SspIdentityPublicKey, 9999999999)
	replay, err := handler.RecoverSSPInstantDeposit(owner, input)
	require.NoError(t, err)
	require.True(t, proto.Equal(result, replay.SpendTxSigningResult))
	other := authn.InjectSessionForTests(ctx, keys.GeneratePrivateKey().Public(), 9999999999)
	_, err = handler.RecoverSSPInstantDeposit(other, input)
	require.Error(t, err)
	changed := proto.Clone(input).(*pbssp.RecoverInstantDepositRequest)
	changed.SpendTxSigningJob.RawTx = []byte("another transaction")
	_, err = handler.RecoverSSPInstantDeposit(owner, changed)
	require.ErrorContains(t, err, "original transaction, nonce")
	changed = proto.Clone(input).(*pbssp.RecoverInstantDepositRequest)
	changed.SpendTxSigningJob.SigningNonceCommitment = claimPrepareOpFor(t, fixture, nil, nil).OriginalRequest.SpendTxSigningJob.SigningNonceCommitment
	_, err = handler.RecoverSSPInstantDeposit(owner, changed)
	require.ErrorContains(t, err, "original transaction, nonce")
}
