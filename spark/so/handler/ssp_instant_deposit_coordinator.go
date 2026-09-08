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
	"github.com/lightsparkdev/spark/so/ent/utxoswap"
	"github.com/lightsparkdev/spark/so/frost"
	"github.com/lightsparkdev/spark/so/helper"
	transferhelper "github.com/lightsparkdev/spark/so/transfer"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type sspInstantReserveCoordinator struct {
	*ReserveInstantStaticDepositFlowHandler
	req  *pbinternal.ReserveInstantStaticDepositUtxoSwapRequest
	send *sendTransferCoordinatorFlow
}

func (f *sspInstantReserveCoordinator) PrepareOp() proto.Message {
	return &pbinternal.ReserveInstantStaticDepositUtxoSwapPrepareRequest{
		OriginalRequest: f.req, SenderKeyTweakProofs: f.send.senderKeyTweakProofs,
	}
}
func (f *sspInstantReserveCoordinator) RollbackPayload() proto.Message {
	return &pbinternal.ReserveInstantStaticDepositUtxoSwapRollbackRequest{RequestedTransferId: f.req.Transfer.TransferId}
}
func (f *sspInstantReserveCoordinator) BuildCommitPayload(ctx context.Context, results map[string]*anypb.Any) (proto.Message, error) {
	commit, err := f.send.BuildCommitPayload(ctx, results)
	if err != nil {
		return nil, err
	}
	return &pbinternal.ReserveInstantStaticDepositUtxoSwapCommitRequest{TransferCommit: commit.(*pbinternal.SendTransferCommitRequest)}, nil
}

func (o *StaticDepositHandler) ReserveSSPInstantDeposit(ctx context.Context, input *pbssp.ReserveInstantDepositRequest) (*pbssp.ReserveInstantDepositResponse, error) {
	if input == nil || input.GetTransfer() == nil || input.GetOnChainUtxo() == nil || input.GetCreditAmountSats() <= 0 {
		return nil, fmt.Errorf("UTXO, transfer and positive credit are required")
	}
	sender, err := keys.ParsePublicKey(input.Transfer.OwnerIdentityPublicKey)
	if err != nil {
		return nil, err
	}
	if err := authz.EnforceSessionIdentityPublicKeyMatches(ctx, o.config, sender); err != nil {
		return nil, err
	}
	receiver, err := keys.ParsePublicKey(input.Transfer.ReceiverIdentityPublicKey)
	if err != nil {
		return nil, err
	}
	network, err := btcnetwork.FromProtoNetwork(input.OnChainUtxo.Network)
	if err != nil {
		return nil, err
	}
	if input.ValueSats < input.CreditAmountSats {
		return nil, fmt.Errorf("credit exceeds deposit value")
	}
	if err := validateInstantUserSignature(receiver, input.UserSignature, input.SspSignature, network,
		uint64(input.CreditAmountSats), 0, input.DestinationAddress, uint64(input.ValueSats)); err != nil {
		return nil, err
	}
	id, err := uuid.Parse(input.Transfer.TransferId)
	if err != nil {
		return nil, err
	}
	db, err := ent.GetDbFromContext(ctx)
	if err != nil {
		return nil, err
	}
	existing, err := db.UtxoSwap.Query().Where(utxoswap.RequestedTransferIDEQ(id), utxoswap.RequestTypeEQ(st.UtxoSwapRequestTypeInstant)).Only(ctx)
	if err != nil && !ent.IsNotFound(err) {
		return nil, err
	}
	if existing != nil {
		if !existing.SspIdentityPublicKey.Equals(sender) ||
			!bytes.Equal(existing.UserIdentityPublicKey.Serialize(), input.Transfer.ReceiverIdentityPublicKey) ||
			!bytes.Equal(existing.SspSignature, input.SspSignature) || !bytes.Equal(existing.UserSignature, input.UserSignature) ||
			existing.CreditAmountSats != uint64(input.CreditAmountSats) || existing.UtxoValueSats != uint64(input.ValueSats) {
			return nil, fmt.Errorf("instant reservation belongs to a different request")
		}
		transfer, _, err := GetTransferFromUtxoSwap(ctx, existing)
		if err != nil {
			return nil, err
		}
		if !transferhelper.IsTransferSent(transfer) {
			return nil, fmt.Errorf("instant reservation transfer is not committed")
		}
		result, err := marshalSSPDepositTransfer(ctx, transfer)
		if err != nil {
			return nil, err
		}
		return &pbssp.ReserveInstantDepositResponse{Transfer: result}, nil
	}
	req := &pbinternal.ReserveInstantStaticDepositUtxoSwapRequest{
		OnChainUtxo: input.OnChainUtxo, SspSignature: input.SspSignature, UserSignature: input.UserSignature,
		Transfer: input.Transfer, DestinationAddress: input.DestinationAddress,
		ValueSats: input.ValueSats, CreditAmountSats: input.CreditAmountSats,
	}
	h := NewReserveInstantStaticDepositFlowHandler(o.config)
	transferReq := convertV2ToV3SendTransferRequest(req.Transfer)
	parsed, err := parseSendTransferRequest(transferReq)
	if err != nil {
		return nil, err
	}
	send, err := buildSendTransferCoordinatorFlow(ctx, o.config, transferReq, parsed, "", h.transfer)
	if err != nil {
		return nil, err
	}
	flow := &sspInstantReserveCoordinator{ReserveInstantStaticDepositFlowHandler: h, req: req, send: send}
	engine, err := consensus.GetEngine(ctx)
	if err != nil {
		return nil, err
	}
	selection := helper.OperatorSelection{Option: helper.OperatorSelectionOptionAll}
	if _, err := engine.Execute(ctx, pbgossip.ConsensusOperationType_CONSENSUS_OPERATION_TYPE_RESERVE_INSTANT_STATIC_DEPOSIT_UTXO_SWAP, &selection, flow); err != nil {
		return nil, err
	}
	if send.response == nil {
		return nil, fmt.Errorf("instant reservation committed without response")
	}
	return &pbssp.ReserveInstantDepositResponse{Transfer: send.response.Transfer}, nil
}

type sspInstantRecoveryCoordinator struct {
	*ClaimInstantStaticDepositFlowHandler
	input             *pbssp.RecoverInstantDepositRequest
	req               *pbinternal.ClaimInstantStaticDepositUtxoSwapRequest
	target            *VerifiedTargetUtxo
	commitments       map[string]*pbcommon.SigningCommitment
	parsedCommitments map[string]frost.SigningCommitment
	response          *pbssp.StaticDepositSwapResponse
}

func (f *sspInstantRecoveryCoordinator) PrepareOp() proto.Message {
	return &pbinternal.ClaimInstantStaticDepositUtxoSwapPrepareRequest{OriginalRequest: f.req, SpendTxSigningCommitments: f.commitments}
}
func (f *sspInstantRecoveryCoordinator) RollbackPayload() proto.Message {
	return &pbinternal.ClaimInstantStaticDepositUtxoSwapRollbackRequest{TransferId: f.req.TransferId}
}
func (f *sspInstantRecoveryCoordinator) BuildCommitPayload(ctx context.Context, results map[string]*anypb.Any) (proto.Message, error) {
	shares, _, err := collectSignatureShares(results)
	if err != nil {
		return nil, err
	}
	address, err := f.target.inner.QueryDepositAddress().Only(ctx)
	if err != nil {
		return nil, err
	}
	keyshare, err := address.QuerySigningKeyshare().Only(ctx)
	if err != nil {
		return nil, err
	}
	verifyingKey := keyshare.PublicKey.Add(address.OwnerSigningPubkey)
	message, _, err := GetTxSigningInfo(ctx, f.target.inner, f.req.SpendTxSigningJob.RawTx)
	if err != nil {
		return nil, err
	}
	nonce := frost.SigningCommitment{}
	if err := nonce.UnmarshalProto(f.req.SpendTxSigningJob.SigningNonceCommitment); err != nil {
		return nil, err
	}
	jobID := claimInstantSpendTxJobID(f.req.OnChainUtxo.Txid, f.req.OnChainUtxo.Vout)
	job := &helper.SigningJob{JobID: jobID, SigningKeyshareID: keyshare.ID, Message: message, VerifyingKey: &verifyingKey, UserCommitment: &nonce}
	var ids []string
	for id := range f.parsedCommitments {
		ids = append(ids, id)
	}
	selection, err := helper.NewPreSelectedOperatorSelection(f.config, ids)
	if err != nil {
		return nil, err
	}
	packages, err := ent.GetKeyPackages(ctx, f.config, []uuid.UUID{keyshare.ID})
	if err != nil {
		return nil, err
	}
	round2, ok := shares[jobID.String()]
	if !ok {
		return nil, fmt.Errorf("missing instant recovery signature shares")
	}
	signing, err := helper.BuildSigningResults(f.config, selection, []*helper.SigningJob{job}, packages,
		[]map[string]frost.SigningCommitment{f.parsedCommitments}, map[uuid.UUID]map[string][]byte{jobID: round2})
	if err != nil {
		return nil, err
	}
	if len(signing) != 1 {
		return nil, fmt.Errorf("expected one instant recovery signing result")
	}
	result := signing[0].MarshalProto()
	db, err := ent.GetDbFromContext(ctx)
	if err != nil {
		return nil, err
	}
	id, err := uuid.Parse(f.req.TransferId)
	if err != nil {
		return nil, err
	}
	swap, err := loadInstantSwapForClaim(ctx, db, id, true, st.UtxoSwapStatusCreated)
	if err != nil {
		return nil, err
	}
	checkpoint, err := proto.Marshal(&pbssp.InstantRecoveryCheckpoint{Request: f.input, SigningResult: result})
	if err != nil {
		return nil, err
	}
	if _, err := swap.Update().SetSpendTxSigningResult(checkpoint).Save(ctx); err != nil {
		return nil, err
	}
	commit := &pbinternal.ClaimInstantStaticDepositUtxoSwapCommitRequest{TransferId: f.req.TransferId}
	if err := f.Commit(ctx, commit); err != nil {
		return nil, err
	}
	f.response, err = instantRecoveryResponse(ctx, swap, result)
	return commit, err
}

func instantRecoveryResponse(ctx context.Context, swap *ent.UtxoSwap, result *pbspark.SigningResult) (*pbssp.StaticDepositSwapResponse, error) {
	transfer, _, err := GetTransferFromUtxoSwap(ctx, swap)
	if err != nil {
		return nil, err
	}
	transferProto, err := marshalSSPDepositTransfer(ctx, transfer)
	if err != nil {
		return nil, err
	}
	coin, err := swap.QueryUtxo().Only(ctx)
	if err != nil {
		return nil, err
	}
	address, err := coin.QueryDepositAddress().Only(ctx)
	if err != nil {
		return nil, err
	}
	key, err := address.QuerySigningKeyshare().Only(ctx)
	if err != nil {
		return nil, err
	}
	return &pbssp.StaticDepositSwapResponse{Transfer: transferProto, SpendTxSigningResult: result,
		DepositAddress: &pbspark.DepositAddressQueryResult{DepositAddress: address.Address,
			UserSigningPublicKey: address.OwnerSigningPubkey.Serialize(), VerifyingPublicKey: key.PublicKey.Add(address.OwnerSigningPubkey).Serialize()}}, nil
}

// The UTXO-swap loader validates the transfer but does not load its participant
// relations. The wire response requires those relations even on a replay.
func marshalSSPDepositTransfer(ctx context.Context, transfer *ent.Transfer) (*pbspark.Transfer, error) {
	var err error
	transfer.Edges.TransferReceivers, err = transfer.QueryTransferReceivers().All(ctx)
	if err != nil {
		return nil, err
	}
	transfer.Edges.TransferSenders, err = transfer.QueryTransferSenders().All(ctx)
	if err != nil {
		return nil, err
	}
	return transfer.MarshalProto(ctx)
}

func (o *StaticDepositHandler) RecoverSSPInstantDeposit(ctx context.Context, input *pbssp.RecoverInstantDepositRequest) (*pbssp.StaticDepositSwapResponse, error) {
	if input == nil || input.GetOnChainUtxo() == nil || input.GetSpendTxSigningJob() == nil || input.GetSpendTxSigningJob().GetSigningNonceCommitment() == nil {
		return nil, fmt.Errorf("UTXO, transfer ID and signing job are required")
	}
	id, err := uuid.Parse(input.TransferId)
	if err != nil {
		return nil, err
	}
	db, err := ent.GetDbFromContext(ctx)
	if err != nil {
		return nil, err
	}
	swap, err := loadInstantSwapForClaim(ctx, db, id, false, st.UtxoSwapStatusCreated, st.UtxoSwapStatusCompleted)
	if err != nil {
		return nil, err
	}
	if err := authz.EnforceSessionIdentityPublicKeyMatches(ctx, o.config, swap.SspIdentityPublicKey); err != nil {
		return nil, err
	}
	if swap.SecondaryCreditAmountSats != nil && *swap.SecondaryCreditAmountSats != 0 {
		return nil, fmt.Errorf("secondary credit is not supported by this API")
	}
	// Read the checkpoint before checking the chain: the output may already be spent.
	if swap.Status == st.UtxoSwapStatusCompleted {
		var checkpoint pbssp.InstantRecoveryCheckpoint
		if err := proto.Unmarshal(swap.SpendTxSigningResult, &checkpoint); err != nil {
			return nil, err
		}
		if !proto.Equal(checkpoint.Request, input) || len(checkpoint.GetSigningResult().GetSignatureShares()) == 0 {
			return nil, fmt.Errorf("recovery retry must use the original transaction, nonce and coordinator")
		}
		return instantRecoveryResponse(ctx, swap, checkpoint.SigningResult)
	}
	h := NewClaimInstantStaticDepositFlowHandler(o.config)
	req := &pbinternal.ClaimInstantStaticDepositUtxoSwapRequest{OnChainUtxo: input.OnChainUtxo, SpendTxSigningJob: input.SpendTxSigningJob, TransferId: input.TransferId}
	_, target, _, err := h.linkUtxoToReservedSwap(ctx, req)
	if err != nil {
		return nil, err
	}
	round1, err := helper.GetSigningCommitments(ctx, o.config, 1, 1)
	if err != nil {
		return nil, err
	}
	flow := &sspInstantRecoveryCoordinator{ClaimInstantStaticDepositFlowHandler: h, input: input, req: req, target: target,
		commitments: make(map[string]*pbcommon.SigningCommitment), parsedCommitments: make(map[string]frost.SigningCommitment)}
	for id, commitments := range round1 {
		if len(commitments) != 1 {
			return nil, fmt.Errorf("invalid nonce count for %s", id)
		}
		flow.commitments[id] = commitments[0].MarshalProto()
		flow.parsedCommitments[id] = commitments[0]
	}
	engine, err := consensus.GetEngine(ctx)
	if err != nil {
		return nil, err
	}
	selection := helper.OperatorSelection{Option: helper.OperatorSelectionOptionAll}
	if _, err := engine.Execute(ctx, pbgossip.ConsensusOperationType_CONSENSUS_OPERATION_TYPE_CLAIM_INSTANT_STATIC_DEPOSIT_UTXO_SWAP, &selection, flow); err != nil {
		return nil, err
	}
	if flow.response == nil {
		return nil, fmt.Errorf("instant recovery committed without response")
	}
	return flow.response, nil
}
