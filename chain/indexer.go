package chain

import (
	"context"
	"fmt"
	"time"

	"github.com/filecoin-project/lotus/chain/types"
	"github.com/go-pg/pg/v10"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/sentinel-visor/lens"
	"github.com/filecoin-project/sentinel-visor/model"
	"github.com/filecoin-project/sentinel-visor/tasks/actorstate"
	"github.com/filecoin-project/sentinel-visor/tasks/indexer"
)

var log = logging.Logger("chain")

var _ indexer.TipSetObserver = (*TipSetIndexer)(nil)

// A TipSetWatcher waits for tipsets and persists their block data into a database.
type TipSetIndexer struct {
	window     time.Duration
	opener     lens.APIOpener
	storage    Storage
	processors map[string]TipSetProcessor
}

func NewTipSetIndexer(o lens.APIOpener, d Storage, window time.Duration, actorCodes []cid.Cid) (*TipSetIndexer, error) {
	// TODO: remove the hackiness of having to create a mostly unused processor
	asp, err := actorstate.NewActorStateProcessor(nil, nil, 0, 0, 0, 0, actorCodes, false)
	if err != nil {
		return nil, xerrors.Errorf("new actor state processor: %w", err)
	}

	return &TipSetIndexer{
		storage: d,
		processors: map[string]TipSetProcessor{
			"blocks":     NewBlockProcessor(),
			"messages":   NewMessageProcessor(o), // does gas outputs too
			"actorstate": NewActorStateProcessor(o, asp),
			"economics":  NewChainEconomicsProcessor(o),
		},
		window: window,
	}, nil
}

func (t *TipSetIndexer) TipSet(ctx context.Context, ts *types.TipSet) error {
	if t.window > 0 {
		// Do as much indexing as possible in the specified time window (usually one epoch when following head of chain)
		// Anything not completed in that time will be marked as incomplete
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, t.window)
		defer cancel()
	}
	start := time.Now()

	// Run each task concurrently
	results := make(chan *TaskResult, len(t.processors))
	for name, p := range t.processors {
		go t.runProcessor(ctx, p, name, ts, results)
	}

	data := make(PersistableWithTxList, 0, len(t.processors))

	// Gather results
	inFlight := len(t.processors)
	for inFlight > 0 {
		res := <-results
		inFlight--
		elapsed := time.Since(start)

		if res.Error != nil {
			log.Errorw("task returned with error", "task", res.Task, "error", res.Error.Error(), "time", elapsed)
			continue
		}

		log.Debugw("task returned with data", "task", res.Task, "time", elapsed)
		data = append(data, res.Data)
	}

	// TODO: persist all returned data asynch

	log.Debugw("tipset data extracted", "time", time.Since(start))
	if err := t.storage.Persist(ctx, data); err != nil {
		log.Errorw("persistence failed", "error", err)
	}

	log.Debugw("tipset complete", "total_time", time.Since(start))

	return nil
}

func (t *TipSetIndexer) runProcessor(ctx context.Context, p TipSetProcessor, name string, ts *types.TipSet, results chan *TaskResult) {
	data, err := p.ProcessTipSet(ctx, ts)
	if err != nil {
		results <- &TaskResult{
			Task:  name,
			Error: ctx.Err(),
		}
		return
	}
	results <- &TaskResult{
		Task: name,
		Data: data,
	}
}

type PersistableWithTxList []model.PersistableWithTx

var _ model.PersistableWithTx = (PersistableWithTxList)(nil)

func (pl PersistableWithTxList) PersistWithTx(ctx context.Context, tx *pg.Tx) error {
	log.Debugw("PersistableWithTxList.PersistWithTx", "count", len(pl))
	for i, p := range pl {
		if p == nil {
			log.Debugw("PersistableWithTxList.PersistWithTx encountered nil item", "index", i)
			continue
		}
		log.Debugw("PersistableWithTxList.PersistWithTx persisting item", "index", i, "type", fmt.Sprintf("%T", p))
		if err := p.PersistWithTx(ctx, tx); err != nil {
			log.Debugw("PersistableWithTxList.PersistWithTx persistence failed", "index", i, "type", fmt.Sprintf("%T", p), "error", err)
			return err
		}
	}
	return nil
}

// A TaskResult is either some data to persist or an error which indicates that the task did not complete
type TaskResult struct {
	Task  string
	Error error
	Data  model.PersistableWithTx
}

type TipSetProcessor interface {
	ProcessTipSet(ctx context.Context, ts *types.TipSet) (model.PersistableWithTx, error)
	Close() error
}
