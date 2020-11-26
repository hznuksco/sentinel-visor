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
	visormodel "github.com/filecoin-project/sentinel-visor/model/visor"
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
			BlocksTask:         NewBlockProcessor(),
			MessagesTask:       NewMessageProcessor(o), // does gas outputs too
			ActorStateTask:     NewActorStateProcessor(o, asp),
			ChainEconomicsTask: NewChainEconomicsProcessor(o),
		},
		window: window,
	}, nil
}

func (t *TipSetIndexer) TipSet(ctx context.Context, ts *types.TipSet) error {
	var cancel func()
	if t.window > 0 {
		// Do as much indexing as possible in the specified time window (usually one epoch when following head of chain)
		// Anything not completed in that time will be marked as incomplete
		ctx, cancel = context.WithTimeout(ctx, t.window)
	} else {
		// Ensure all goroutines are stopped when we exit
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	start := time.Now()

	// Run each task concurrently
	results := make(chan *TaskResult, len(t.processors))
	for name, p := range t.processors {
		go t.runProcessor(ctx, p, name, ts, results)
	}

	data := make(PersistableWithTxList, 0, len(t.processors))

	ll := log.With("height", int64(ts.Height()))

	// Gather results
	inFlight := len(t.processors)
	for inFlight > 0 {
		res := <-results
		inFlight--

		// Was there a fatal error?
		if res.Error != nil {
			ll.Errorw("task returned with error", "task", res.Task, "error", res.Error.Error())
			return res.Error
		}

		if res.Report == nil {
			// Nothing was done for this tipset
			ll.Debugw("task returned with no report", "task", res.Task)
			continue
		}

		// Fill in some report metadata
		res.Report.StartedAt = start
		res.Report.CompletedAt = time.Now()

		if res.Report.ErrorsDetected != nil {
			res.Report.Status = visormodel.ProcessingStatusError
		} else if res.Report.StatusInformation != "" {
			res.Report.Status = visormodel.ProcessingStatusInfo
		} else {
			res.Report.Status = visormodel.ProcessingStatusOK
		}

		ll.Infow("task report", "task", res.Task, "status", res.Report.Status, "time", res.Report.CompletedAt.Sub(res.Report.StartedAt))

		// Persist the processing report and the data
		data = append(data, PersistableWithTxList{res.Report, res.Data})
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
	data, report, err := p.ProcessTipSet(ctx, ts)
	if err != nil {
		results <- &TaskResult{
			Task:  name,
			Error: err,
		}
		return
	}
	results <- &TaskResult{
		Task:   name,
		Report: report,
		Data:   data,
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

// A TaskResult is either some data to persist or an error which indicates that the task did not complete. Partial
// completions are possible provided the Data contains a persistable log of the results.
type TaskResult struct {
	Task   string
	Error  error
	Report *visormodel.ProcessingReport
	Data   model.PersistableWithTx
}

type TipSetProcessor interface {
	// ProcessTipSet processes a tipset. If error is non-nil then the processor encountered a fatal error.
	// Any data returned must be accompanied by a processing report.
	ProcessTipSet(ctx context.Context, ts *types.TipSet) (model.PersistableWithTx, *visormodel.ProcessingReport, error)
	Close() error
}
