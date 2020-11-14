package indexer

import (
	"context"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/chain/types"
	"go.opencensus.io/tag"
	"go.opentelemetry.io/otel/api/global"
	"go.opentelemetry.io/otel/api/trace"
	"go.opentelemetry.io/otel/label"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/sentinel-visor/lens"
	"github.com/filecoin-project/sentinel-visor/metrics"
)

func NewChainHistoryIndexer(obs TipSetObserver, opener lens.APIOpener, minHeight, maxHeight int64) *ChainHistoryIndexer {
	return &ChainHistoryIndexer{
		opener:    opener,
		obs:       obs,
		finality:  900,
		minHeight: minHeight,
		maxHeight: maxHeight,
	}
}

// ChainHistoryIndexer is a task that indexes blocks by following the chain history.
type ChainHistoryIndexer struct {
	opener    lens.APIOpener
	obs       TipSetObserver
	finality  int   // epochs after which chain state is considered final
	minHeight int64 // limit persisting to tipsets equal to or above this height
	maxHeight int64 // limit persisting to tipsets equal to or below this height}
}

// Run starts walking the chain history and continues until the context is done or
// the start of the chain is reached.
func (c *ChainHistoryIndexer) Run(ctx context.Context) error {
	node, closer, err := c.opener.Open(ctx)
	if err != nil {
		return xerrors.Errorf("open lens: %w", err)
	}
	defer closer()

	if err := c.WalkChain(ctx, node); err != nil {
		return xerrors.Errorf("walk chain: %w", err)
	}

	return nil
}

func (c *ChainHistoryIndexer) WalkChain(ctx context.Context, node lens.API) error {
	ctx, span := global.Tracer("").Start(ctx, "ChainHistoryIndexer.WalkChain", trace.WithAttributes(label.Int64("height", c.maxHeight)))
	defer span.End()

	ctx, _ = tag.New(ctx, tag.Upsert(metrics.TaskType, "indexhistoryblock"))

	ts, err := node.ChainGetTipSetByHeight(ctx, abi.ChainEpoch(c.maxHeight), types.EmptyTSK)
	if err != nil {
		return xerrors.Errorf("get tipset by height: %w", err)
	}

	log.Debugw("found tipset", "height", ts.Height())
	c.obs.TipSet(ctx, ts)

	for int64(ts.Height()) >= c.minHeight && ts.Height() > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		ts, err = node.ChainGetTipSet(ctx, ts.Parents())
		if err != nil {
			return xerrors.Errorf("get tipset: %w", err)
		}

		log.Debugw("found tipset", "height", ts.Height())
		c.obs.TipSet(ctx, ts)

	}

	return nil
}
