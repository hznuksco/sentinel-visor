package lensutil

import (
	"context"

	"github.com/filecoin-project/lotus/chain/store"
	"github.com/filecoin-project/lotus/chain/types"
	blockadt "github.com/filecoin-project/specs-actors/actors/util/adt"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/sentinel-visor/lens"
)

// GetMessagesForTipset returns a list of messages sent as part of pts (parent) with receipts found in ts (child).
// No attempt at deduplication of messages is made.
func GetExecutedMessageForTipset(ctx context.Context, cs *store.ChainStore, ts, pts *types.TipSet) ([]*lens.ExecutedMessage, error) {
	if !types.CidArrsEqual(ts.Parents().Cids(), pts.Cids()) {
		return nil, xerrors.Errorf("child is not on the same chain")
	}

	bmsgs, err := cs.BlockMsgsForTipset(pts)
	if err != nil {
		return nil, xerrors.Errorf("block messages for tipset: %w", err)
	}

	pblocks := pts.Blocks()
	if len(bmsgs) != len(pblocks) {
		// logic error somewhere
		return nil, xerrors.Errorf("mismatching number of blocks returned from block messages, got %d wanted %d", len(bmsgs), len(pblocks))
	}

	count := 0
	for _, bm := range bmsgs {
		count += len(bm.BlsMessages) + len(bm.SecpkMessages)
	}

	// Start building a list of completed message with receipt
	emsgs := make([]*lens.ExecutedMessage, 0, count)

	// bmsgs is ordered by block
	for blockIdx, bm := range bmsgs {
		for _, blsm := range bm.BlsMessages {
			emsgs = append(emsgs, &lens.ExecutedMessage{
				Cid:      blsm.Cid(),
				Height:   pts.Height(),
				Msg:      blsm.VMMessage(),
				Block:    pblocks[blockIdx],
				BlockCid: pts.Cids()[blockIdx],
			})
		}

		for _, secm := range bm.SecpkMessages {
			emsgs = append(emsgs, &lens.ExecutedMessage{
				Cid:      secm.Cid(),
				Height:   pts.Height(),
				Msg:      secm.VMMessage(),
				Block:    pblocks[blockIdx],
				BlockCid: pts.Cids()[blockIdx],
			})
		}

	}

	// Retrieve receipts using a block from the child tipset
	rs, err := blockadt.AsArray(cs.Store(ctx), ts.Blocks()[0].ParentMessageReceipts)
	if err != nil {
		return nil, xerrors.Errorf("amt load: %w", err)
	}

	if rs.Length() != uint64(len(emsgs)) {
		// logic error somewhere
		return nil, xerrors.Errorf("mismatching number of receipts: got %d wanted %d", rs.Length(), len(emsgs))
	}

	// Receipts are in same order as BlockMsgsForTipset
	for i := uint64(0); i < rs.Length(); i++ {
		var r types.MessageReceipt
		if found, err := rs.Get(uint64(i), &r); err != nil {
			return nil, err
		} else if !found {
			return nil, xerrors.Errorf("failed to find receipt %d", i)
		}

		emsgs[i].Rcpt = &r
	}

	return emsgs, nil
}
