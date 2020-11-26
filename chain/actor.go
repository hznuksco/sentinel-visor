package chain

import (
	"context"
	"fmt"
	"time"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lotus/chain/state"
	"github.com/filecoin-project/lotus/chain/types"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/sentinel-visor/lens"
	"github.com/filecoin-project/sentinel-visor/model"
	commonmodel "github.com/filecoin-project/sentinel-visor/model/actors/common"
	"github.com/filecoin-project/sentinel-visor/tasks/actorstate"
)

type ActorStateProcessor struct {
	asp           *actorstate.ActorStateProcessor
	node          lens.API
	opener        lens.APIOpener
	closer        lens.APICloser
	lastTipSet    *types.TipSet
	lastStateTree *state.StateTree
}

func NewActorStateProcessor(opener lens.APIOpener, asp *actorstate.ActorStateProcessor) *ActorStateProcessor {
	return &ActorStateProcessor{
		asp:    asp,
		opener: opener,
	}
}

func (p *ActorStateProcessor) ProcessTipSet(ctx context.Context, ts *types.TipSet) (model.PersistableWithTx, error) {
	if p.node == nil {
		node, closer, err := p.opener.Open(ctx)
		if err != nil {
			return nil, xerrors.Errorf("unable to open lens: %w", err)
		}
		p.node = node
		p.closer = closer
	}

	var result model.PersistableWithTx
	var err error

	stateTree, err := state.LoadStateTree(p.node.Store(), ts.ParentState())
	if err != nil {
		return nil, xerrors.Errorf("failed to load state tree: %w", err)
	}

	if p.lastTipSet != nil && p.lastStateTree != nil {
		if p.lastTipSet.Height() > ts.Height() {
			// last tipset seen was the child
			result, err = p.processStateChanges(ctx, p.lastTipSet, ts, p.lastStateTree, stateTree)
		} else if p.lastTipSet.Height() < ts.Height() {
			// last tipset seen was the parent
			result, err = p.processStateChanges(ctx, ts, p.lastTipSet, stateTree, p.lastStateTree)
		} else {
			// TODO: record in database that we were unable to process actors for this tipset
			log.Errorw("out of order tipsets", "height", ts.Height(), "last_height", p.lastTipSet.Height())
		}
	}

	p.lastTipSet = ts
	p.lastStateTree = stateTree

	// TODO: close lens if rpc error
	return result, err
}

func (p *ActorStateProcessor) processStateChanges(ctx context.Context, ts *types.TipSet, pts *types.TipSet, stateTree *state.StateTree, parentStateTree *state.StateTree) (model.PersistableWithTx, error) {
	log.Debugw("processing state changes", "height", ts.Height(), "parent_height", pts.Height())
	if !types.CidArrsEqual(ts.Parents().Cids(), pts.Cids()) {
		return nil, xerrors.Errorf("child is not on the same chain")
	}

	changes, err := state.Diff(parentStateTree, stateTree)
	if err != nil {
		return nil, xerrors.Errorf("get actor changes: %w", err)
	}

	ll := log.With("height", int64(ts.Height()))

	ll.Debugw("found actor state changes", "count", len(changes))

	rawResults := make(PersistableWithTxList, len(changes))
	parsedResults := make(PersistableWithTxList, len(changes))

	grp, ctx := errgroup.WithContext(ctx)

	idx := 0
	for str, act := range changes {
		lla := ll.With("addr", str, "code", actorstate.ActorNameByCode(act.Code))
		lla.Debugw("found actor change")

		extracter, ok := actorstate.GetActorStateExtractor(act.Code)
		if !ok {
			lla.Debugw("skipping change for unsupported actor")
			continue
		}

		addr, err := address.NewFromString(str)
		if err != nil {
			return nil, xerrors.Errorf("parse address: %w", err)
		}

		info := actorstate.ActorInfo{
			Actor:           act,
			Address:         addr,
			ParentStateRoot: pts.ParentState(),
			Epoch:           ts.Height(),
			TipSet:          pts.Key(),
			ParentTipSet:    pts.Parents(),
		}

		grp.Go(func() error {
			start := time.Now()
			lla.Debugw("parsing actor", "idx", idx, "actor", info.Actor, "address", info.Address, "height", info.Epoch, "tipset", info.TipSet, "parent_tipset", info.ParentTipSet)

			// TODO: we have the state trees, can we optimize actor state extraction further?

			// Extract raw state
			var ae actorstate.ActorExtractor
			raw, err := ae.Extract(ctx, info, p.node)
			if err != nil {
				return xerrors.Errorf("extract raw actor state: %w", err)
			}

			// Parse state
			parsed, err := extracter.Extract(ctx, info, p.node)
			if err != nil {
				return xerrors.Errorf("extract actor state: %w", err)
			}

			lla.Debugw("parsed actor change", "time", time.Since(start), "idx", idx)
			rawResults[idx] = raw
			parsedResults[idx] = parsed
			return nil
		})

		idx++

	}

	if err := grp.Wait(); err != nil {
		return nil, err
	}

	for i, r := range rawResults {
		if r == nil {
			ll.Errorw("missing raw actor state", "idx", i)
			continue
		}

		atr, ok := r.(*commonmodel.ActorTaskResult)
		if !ok {
			ll.Errorw(fmt.Sprintf("expected *commonmodel.ActorTaskResult but got %T", r), "idx", i)
			continue
		}

		ll.Debugw(fmt.Sprintf("raw actor data: %+v", atr.Actor), "idx", idx)
		ll.Debugw(fmt.Sprintf("parsed actor data: %+v", atr.State), "idx", idx)

	}

	return PersistableWithTxList{
		rawResults,
		parsedResults,
	}, nil
}

func (p *ActorStateProcessor) Close() error {
	if p.closer != nil {
		p.closer()
	}
	return nil
}
