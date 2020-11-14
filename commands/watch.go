package commands

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/filecoin-project/sentinel-visor/schedule"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/sentinel-visor/chain"
	"github.com/filecoin-project/sentinel-visor/tasks/indexer"
)

var Watch = &cli.Command{
	Name:  "watch",
	Usage: "Watch the head of the filecoin blockchain and process blocks as they arrive.",
	Flags: []cli.Flag{
		&cli.IntFlag{
			Name:    "indexhead-confidence",
			Usage:   "Sets the size of the cache used to hold tipsets for possible reversion before being committed to the database",
			Value:   2,
			EnvVars: []string{"VISOR_INDEXHEAD_CONFIDENCE"},
		},
		&cli.BoolFlag{
			Name:   "nopersist",
			Usage:  "Disable persistence of data to postgres",
			Hidden: true,
		},
	},
	Action: watch,
}

func watch(cctx *cli.Context) error {
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

	actorCodes, err := getActorCodes(cctx)
	if err != nil {
		return err
	}

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

	tsIndexer, err := chain.NewTipSetIndexer(lensOpener, storage, builtin.EpochDurationSeconds*time.Second, actorCodes)
	if err != nil {
		return xerrors.Errorf("setup indexer: %w", err)
	}

	scheduler := schedule.NewScheduler(cctx.Duration("task-delay"))
	scheduler.Add(schedule.TaskConfig{
		Name: "ChainHeadIndexer",
		Task: indexer.NewChainHeadIndexer(tsIndexer, lensOpener, cctx.Int("indexhead-confidence")),
		// Locker:              NewGlobalSingleton(ChainHeadIndexerLockID, rctx.db), // only want one forward indexer anywhere to be running
		RestartOnFailure:    true,
		RestartOnCompletion: true, // we always want the indexer to be running
		RestartDelay:        time.Minute,
	})

	// Start the scheduler and wait for it to complete or to be cancelled.
	err = scheduler.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}
