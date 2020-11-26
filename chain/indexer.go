package chain

import (
	"context"
	"time"

	"github.com/filecoin-project/lotus/chain/types"
	"github.com/go-pg/pg/v10"
	logging "github.com/ipfs/go-log/v2"

	"github.com/filecoin-project/sentinel-visor/lens"
	"github.com/filecoin-project/sentinel-visor/model"
	visormodel "github.com/filecoin-project/sentinel-visor/model/visor"
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
	name       string
}

func NewTipSetIndexer(o lens.APIOpener, d Storage, window time.Duration, name string) (*TipSetIndexer, error) {
	return &TipSetIndexer{
		storage: d,
		processors: map[string]TipSetProcessor{
			BlocksTask:         NewBlockProcessor(),
			MessagesTask:       NewMessageProcessor(o), // does gas outputs too
			ActorStateTask:     NewActorStateProcessor(o),
			ChainEconomicsTask: NewChainEconomicsProcessor(o),
		},
		window: window,
		name:   name,
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
		res.Report.Reporter = t.name
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
		if err := t.storage.Persist(ctx, PersistableWithTxList{res.Report, res.Data}); err != nil {
			ll.Errorw("persistence failed", "error", err)
		}
		log.Debugw("task data persisted", "time", time.Since(start))
	}

	// TODO: persist all returned data asynch
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
	for _, p := range pl {
		if p == nil {
			continue
		}
		if err := p.PersistWithTx(ctx, tx); err != nil {
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
