package chain

import (
	"context"

	"github.com/filecoin-project/lotus/chain/types"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/sentinel-visor/lens"
	"github.com/filecoin-project/sentinel-visor/model"
	"github.com/filecoin-project/sentinel-visor/tasks/chain"
)

type ChainEconomicsProcessor struct {
	extracter *chain.ChainEconomicsExtracter
	node      lens.API
	opener    lens.APIOpener
	closer    lens.APICloser
}

func NewChainEconomicsProcessor(opener lens.APIOpener) *ChainEconomicsProcessor {
	return &ChainEconomicsProcessor{
		opener:    opener,
		extracter: &chain.ChainEconomicsExtracter{},
	}
}

func (p *ChainEconomicsProcessor) ProcessTipSet(ctx context.Context, ts *types.TipSet) (model.PersistableWithTx, error) {
	if p.node == nil {
		node, closer, err := p.opener.Open(ctx)
		if err != nil {
			return nil, xerrors.Errorf("unable to open lens: %w", err)
		}
		p.node = node
		p.closer = closer
	}
	// TODO: close lens if rpc error

	ce, err := p.extracter.ProcessTipSet(ctx, p.node, ts)
	if err != nil {
		return nil, xerrors.Errorf("process tip set: %w", err)
	}

	return ce, nil
}

func (p *ChainEconomicsProcessor) Close() error {
	if p.closer != nil {
		p.closer()
	}
	return nil
}
