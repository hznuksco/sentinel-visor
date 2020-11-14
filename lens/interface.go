package lens

import (
	"context"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/vm"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/ipfs/go-cid"
)

type API interface {
	Store() adt.Store
	api.FullNode
	ComputeGasOutputs(gasUsed, gasLimit int64, baseFee, feeCap, gasPremium abi.TokenAmount) vm.GasOutputs
	GetExecutedMessageForTipset(ctx context.Context, ts, pts *types.TipSet) ([]*ExecutedMessage, error)
}

type APICloser func()

type APIOpener interface {
	Open(context.Context) (API, APICloser, error)
}

type ExecutedMessage struct {
	Cid      cid.Cid
	Height   abi.ChainEpoch
	Msg      *types.Message
	Rcpt     *types.MessageReceipt
	Block    *types.BlockHeader
	BlockCid cid.Cid
}
