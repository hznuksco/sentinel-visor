package commands

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/filecoin-project/sentinel-visor/schedule"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/sentinel-visor/chain"
	"github.com/filecoin-project/sentinel-visor/tasks/indexer"
)

var Walk = &cli.Command{
	Name:  "walk",
	Usage: "Walk a range of the filecoin blockchain and process blocks as they are discovered.",
	Flags: []cli.Flag{
		&cli.Int64Flag{
			Name:    "from",
			Usage:   "Limit actor and message processing to tipsets at or above `HEIGHT`",
			EnvVars: []string{"VISOR_HEIGHT_FROM"},
		},
		&cli.Int64Flag{
			Name:        "to",
			Usage:       "Limit actor and message processing to tipsets at or below `HEIGHT`",
			Value:       estimateCurrentEpoch(),
			DefaultText: "MaxInt64",
			EnvVars:     []string{"VISOR_HEIGHT_TO"},
		},
	},
	Action: walk,
}

func walk(cctx *cli.Context) error {
	// Validate flags
	heightFrom := cctx.Int64("from")
	heightTo := cctx.Int64("to")

	if heightFrom > heightTo {
		return xerrors.Errorf("--from must not be greater than --to")
	}

	if err := setupLogging(cctx); err != nil {
		return xerrors.Errorf("setup logging: %w", err)
	}

	if err := setupMetrics(cctx); err != nil {
		return xerrors.Errorf("setup metrics: %w", err)
	}

	tcloser, err := setupTracing(cctx)
	if err != nil {
		return xerrors.Errorf("setup tracing: %w", err)
	}
	defer tcloser()

	lensOpener, lensCloser, err := setupLens(cctx)
	if err != nil {
		return xerrors.Errorf("setup lens: %w", err)
	}
	defer func() {
		lensCloser()
	}()

	var storage chain.Storage = &chain.NullStorage{}
	if cctx.String("db") == "" {
		log.Warnw("database not specified, data will not be persisted")
	} else {
		db, err := setupDatabase(cctx)
		if err != nil {
			return xerrors.Errorf("setup database: %w", err)
		}
		storage = db
	}

	// Set up a context that is canceled when the command is interrupted
	ctx, cancel := context.WithCancel(cctx.Context)
	defer cancel()

	// Set up a signal handler to cancel the context
	go func() {
		interrupt := make(chan os.Signal, 1)
		signal.Notify(interrupt, syscall.SIGTERM, syscall.SIGINT)
		select {
		case <-interrupt:
			cancel()
		case <-ctx.Done():
		}
	}()

	scheduler := schedule.NewScheduler(cctx.Duration("task-delay"))

	tsIndexer, err := chain.NewTipSetIndexer(lensOpener, storage, 0, getInstanceIdentifier(cctx))
	if err != nil {
		return xerrors.Errorf("setup indexer: %w", err)
	}

	scheduler.Add(schedule.TaskConfig{
		Name:                "ChainHistoryIndexer",
		Task:                indexer.NewChainHistoryIndexer(tsIndexer, lensOpener, heightFrom, heightTo),
		RestartOnFailure:    true,
		RestartOnCompletion: false,
		RestartDelay:        time.Minute,
	})

	// Start the scheduler and wait for it to complete or to be cancelled.
	err = scheduler.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}
