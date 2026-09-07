package handler

import (
	"bytes"
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/lightsparkdev/spark/common/btcnetwork"
	"github.com/lightsparkdev/spark/common/keys"
	pbcommon "github.com/lightsparkdev/spark/proto/common"
	pbgossip "github.com/lightsparkdev/spark/proto/gossip"
	pbspark "github.com/lightsparkdev/spark/proto/spark"
	pbinternal "github.com/lightsparkdev/spark/proto/spark_internal"
	pbssp "github.com/lightsparkdev/spark/proto/spark_ssp_internal"
	"github.com/lightsparkdev/spark/so/authz"
	"github.com/lightsparkdev/spark/so/consensus"
	"github.com/lightsparkdev/spark/so/ent"
	st "github.com/lightsparkdev/spark/so/ent/schema/schematype"
	"github.com/lightsparkdev/spark/so/frost"
	"github.com/lightsparkdev/spark/so/helper"
	"github.com/lightsparkdev/spark/so/staticdeposit"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// The existing participant flow commits UTXO ownership and the SSP payout in
// one transaction on each operator. This coordinator supplies the missing
// authenticated entry point and retains signing results for lost replies.
type sspStaticDepositCoordinator struct {
	*StaticDepositUtxoSwapFlowHandler
	req                      *pbinternal.InitiateStaticDepositUtxoSwapRequest
	targetUtxo               *VerifiedTargetUtxo
	signingCommitments       map[string]*pbcommon.SigningCommitment
	signingCommitmentsParsed map[string]frost.SigningCommitment
	send                     *sendTransferCoordinatorFlow
	response                 *pbssp.StaticDepositSwapResponse
}

func (f *sspStaticDepositCoordinator) PrepareOp() proto.Message {
	return &pbinternal.StaticDepositUtxoSwapPrepareRequest{OriginalRequest: f.req, SpendTxSigningCommitments: f.signingCommitments, SenderKeyTweakProofs: f.send.senderKeyTweakProofs}
}
func (f *sspStaticDepositCoordinator) RollbackPayload() proto.Message {
	return &pbinternal.StaticDepositUtxoSwapRollbackRequest{OnChainUtxo: f.req.GetOnChainUtxo()}
}
func (f *sspStaticDepositCoordinator) BuildCommitPayload(ctx context.Context, results map[string]*anypb.Any) (proto.Message, error) {
	allShares, _, err := collectSignatureShares(results)
	if err != nil {
		return nil, fmt.Errorf("failed to collect signature shares: %w", err)
	}

	db, err := ent.GetDbFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get db: %w", err)
	}
	targetUtxo := f.targetUtxo
	depositAddress, err := targetUtxo.inner.QueryDepositAddress().Only(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to get utxo deposit address: %w", err)
	}
	signingKeyshare, err := depositAddress.QuerySigningKeyshare().Only(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get signing keyshare: %w", err)
	}
	verifyingKey := signingKeyshare.PublicKey.Add(depositAddress.OwnerSigningPubkey)
	spendTxSighash, _, err := GetTxSigningInfo(ctx, targetUtxo.inner, f.req.GetSpendTxSigningJob().GetRawTx())
	if err != nil {
		return nil, fmt.Errorf("failed to get spend tx sighash: %w", err)
	}
	userNonce := frost.SigningCommitment{}
	if err := userNonce.UnmarshalProto(f.req.GetSpendTxSigningJob().GetSigningNonceCommitment()); err != nil {
		return nil, fmt.Errorf("failed to parse user nonce commitment: %w", err)
	}

	jobID := staticDepositSwapJobID(f.req.GetOnChainUtxo().GetTxid(), f.req.GetOnChainUtxo().GetVout())
	job := &helper.SigningJob{
		JobID:             jobID,
		SigningKeyshareID: signingKeyshare.ID,
		Message:           spendTxSighash,
		VerifyingKey:      &verifyingKey,
		UserCommitment:    &userNonce,
	}

	operatorIDs := make([]string, 0, len(f.signingCommitmentsParsed))
	for id := range f.signingCommitmentsParsed {
		operatorIDs = append(operatorIDs, id)
	}
	selection, err := helper.NewPreSelectedOperatorSelection(f.config, operatorIDs)
	if err != nil {
		return nil, fmt.Errorf("unable to build signing operator selection: %w", err)
	}
	keyPackages, err := ent.GetKeyPackages(ctx, f.config, []uuid.UUID{signingKeyshare.ID})
	if err != nil {
		return nil, fmt.Errorf("unable to get key packages: %w", err)
	}

	round2, ok := allShares[jobID.String()]
	if !ok {
		return nil, fmt.Errorf("no round-2 shares collected for refund job %s", jobID)
	}
	signingResults, err := helper.BuildSigningResults(
		f.config, selection,
		[]*helper.SigningJob{job}, keyPackages,
		[]map[string]frost.SigningCommitment{f.signingCommitmentsParsed},
		map[uuid.UUID]map[string][]byte{jobID: round2},
	)
	if err != nil {
		return nil, fmt.Errorf("unable to build signing result: %w", err)
	}
	if len(signingResults) == 0 {
		return nil, fmt.Errorf("no signing result produced for refund job %s", jobID)
	}
	signingResultProto := signingResults[0].MarshalProto()

	swap, err := staticdeposit.GetRegisteredUtxoSwapForUtxo(ctx, db, targetUtxo.inner)
	if err != nil {
		return nil, fmt.Errorf("unable to load coordinator utxo swap for %x:%d: %w", f.req.GetOnChainUtxo().GetTxid(), f.req.GetOnChainUtxo().GetVout(), err)
	}
	if swap == nil {
		return nil, fmt.Errorf("coordinator utxo swap not found for %x:%d after prepare", f.req.GetOnChainUtxo().GetTxid(), f.req.GetOnChainUtxo().GetVout())
	}
	signingResultBytes, err := proto.Marshal(signingResultProto)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal signing result bytes: %w", err)
	}
	if _, err := swap.Update().SetSpendTxSigningResult(signingResultBytes).Save(ctx); err != nil {
		return nil, fmt.Errorf("unable to store spend tx signing result: %w", err)
	}
	transferCommit, err := f.send.BuildCommitPayload(ctx, results)
	if err != nil {
		return nil, err
	}
	commit := &pbinternal.StaticDepositUtxoSwapCommitRequest{OnChainUtxo: f.req.GetOnChainUtxo(), TransferCommit: transferCommit.(*pbinternal.SendTransferCommitRequest)}
	if err := f.completeFixedSwap(ctx, f.req.GetOnChainUtxo()); err != nil {
		return nil, err
	}
	f.response = &pbssp.StaticDepositSwapResponse{Transfer: f.send.response.GetTransfer(), SpendTxSigningResult: signingResultProto, DepositAddress: &pbspark.DepositAddressQueryResult{DepositAddress: depositAddress.Address, UserSigningPublicKey: depositAddress.OwnerSigningPubkey.Serialize(), VerifyingPublicKey: verifyingKey.Serialize()}}
	return commit, nil
}

func (o *StaticDepositHandler) InitiateSSPStaticDepositSwap(ctx context.Context, input *pbssp.StaticDepositSwapRequest) (*pbssp.StaticDepositSwapResponse, error) {
	if input == nil || input.GetTransfer() == nil || input.GetOnChainUtxo() == nil || input.GetSpendTxSigningJob() == nil || input.GetSpendTxSigningJob().GetSigningNonceCommitment() == nil {
		return nil, fmt.Errorf("UTXO, transfer and signing job are required")
	}
	sender, err := keys.ParsePublicKey(input.GetTransfer().GetOwnerIdentityPublicKey())
	if err != nil {
		return nil, err
	}
	if err := authz.EnforceSessionIdentityPublicKeyMatches(ctx, o.config, sender); err != nil {
		return nil, err
	}
	req := &pbinternal.InitiateStaticDepositUtxoSwapRequest{OnChainUtxo: input.OnChainUtxo, SspSignature: input.SspSignature, UserSignature: input.UserSignature, Transfer: input.Transfer, SpendTxSigningJob: input.SpendTxSigningJob, HashVariant: input.HashVariant, ConfirmationThreshold: input.ConfirmationThreshold}
	h := NewStaticDepositUtxoSwapFlowHandler(o.config)
	existing, err := h.loadFixedSwapForUtxo(ctx, req.OnChainUtxo)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		if !existing.SspIdentityPublicKey.Equals(sender) || existing.RequestedTransferID.String() != req.Transfer.TransferId || !bytes.Equal(existing.SspSignature, req.SspSignature) || !bytes.Equal(existing.UserSignature, req.UserSignature) {
			return nil, fmt.Errorf("deposit already belongs to another claim")
		}
		if existing.Status != st.UtxoSwapStatusCompleted {
			return nil, fmt.Errorf("deposit consensus is still pending")
		}
		var signingResult pbspark.SigningResult
		if err := proto.Unmarshal(existing.SpendTxSigningResult, &signingResult); err != nil {
			return nil, err
		}
		if len(signingResult.SignatureShares) == 0 {
			return nil, fmt.Errorf("completed deposit has no signing result on this coordinator")
		}
		transfer, _, err := GetTransferFromUtxoSwap(ctx, existing)
		if err != nil {
			return nil, err
		}
		transferProto, err := transfer.MarshalProto(ctx)
		if err != nil {
			return nil, err
		}
		utxo, err := existing.QueryUtxo().Only(ctx)
		if err != nil {
			return nil, err
		}
		address, err := utxo.QueryDepositAddress().Only(ctx)
		if err != nil {
			return nil, err
		}
		keyshare, err := address.QuerySigningKeyshare().Only(ctx)
		if err != nil {
			return nil, err
		}
		return &pbssp.StaticDepositSwapResponse{Transfer: transferProto, SpendTxSigningResult: &signingResult, DepositAddress: &pbspark.DepositAddressQueryResult{DepositAddress: address.Address, UserSigningPublicKey: address.OwnerSigningPubkey.Serialize(), VerifyingPublicKey: keyshare.PublicKey.Add(address.OwnerSigningPubkey).Serialize()}}, nil
	}
	db, err := ent.GetDbFromContext(ctx)
	if err != nil {
		return nil, err
	}
	network, err := btcnetwork.FromProtoNetwork(req.OnChainUtxo.Network)
	if err != nil {
		return nil, err
	}
	if req.ConfirmationThreshold != nil && *req.ConfirmationThreshold == 0 {
		return nil, fmt.Errorf("confirmation threshold must be positive")
	}
	threshold := resolveConfirmationThreshold(req.ConfirmationThreshold, o.config, network)
	target, err := VerifiedTargetUtxoFromRequest(ctx, o.config, db, network, req.OnChainUtxo, &threshold)
	if err != nil {
		return nil, err
	}
	if err := validateStaticDepositSpendTxSpendsTargetUtxo(target, req.SpendTxSigningJob.RawTx); err != nil {
		return nil, err
	}
	address, err := target.inner.QueryDepositAddress().Only(ctx)
	if err != nil {
		return nil, err
	}
	// Reject wrong authorizations before consuming cross-operator nonces.
	leafRefunds := make(map[string][]byte)
	for _, leaf := range req.GetTransfer().GetTransferPackage().GetLeavesToSend() {
		leafRefunds[leaf.LeafId] = leaf.RawTx
	}
	leaves, leafNetwork, err := loadLeaves(ctx, db, leafRefunds, false)
	if err != nil {
		return nil, err
	}
	if len(leaves) == 0 || network != leafNetwork || !address.IsStatic {
		return nil, fmt.Errorf("invalid static deposit transfer")
	}
	amount := getTotalTransferValue(leaves)
	if err := validateUserSignature(address.OwnerIdentityPubkey, req.UserSignature, req.SspSignature, pbspark.UtxoSwapRequestType_Fixed, network, target.Hash().String(), target.Vout(), amount, req.HashVariant); err != nil {
		return nil, err
	}
	transferRequest := convertV2ToV3SendTransferRequest(req.Transfer)
	parsed, err := parseSendTransferRequest(transferRequest)
	if err != nil {
		return nil, err
	}
	send, err := buildSendTransferCoordinatorFlow(ctx, o.config, transferRequest, parsed, "", h.transfer)
	if err != nil {
		return nil, err
	}
	round1, err := helper.GetSigningCommitments(ctx, o.config, 1, 1)
	if err != nil {
		return nil, err
	}
	flow := &sspStaticDepositCoordinator{StaticDepositUtxoSwapFlowHandler: h, req: req, targetUtxo: target, send: send, signingCommitments: make(map[string]*pbcommon.SigningCommitment), signingCommitmentsParsed: make(map[string]frost.SigningCommitment)}
	for id, commitments := range round1 {
		if len(commitments) != 1 {
			return nil, fmt.Errorf("invalid nonce count for %s", id)
		}
		flow.signingCommitments[id] = commitments[0].MarshalProto()
		flow.signingCommitmentsParsed[id] = commitments[0]
	}
	engine, err := consensus.GetEngine(ctx)
	if err != nil {
		return nil, err
	}
	selection := helper.OperatorSelection{Option: helper.OperatorSelectionOptionAll}
	if _, err := engine.Execute(ctx, pbgossip.ConsensusOperationType_CONSENSUS_OPERATION_TYPE_STATIC_DEPOSIT_UTXO_SWAP, &selection, flow); err != nil {
		return nil, err
	}
	if flow.response == nil {
		return nil, fmt.Errorf("deposit consensus completed without response")
	}
	return flow.response, nil
}
