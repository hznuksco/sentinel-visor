package reward

import (
	"context"

	"github.com/go-pg/pg/v10"
	"go.opentelemetry.io/otel/api/global"

	"github.com/filecoin-project/sentinel-visor/metrics"
)

type ChainReward struct {
	Height                            int64  `pg:",pk,notnull,use_zero"`
	StateRoot                         string `pg:",pk,notnull"`
	CumSumBaseline                    string `pg:",notnull"`
	CumSumRealized                    string `pg:",notnull"`
	EffectiveBaselinePower            string `pg:",notnull"`
	NewBaselinePower                  string `pg:",notnull"`
	NewRewardSmoothedPositionEstimate string `pg:",notnull"`
	NewRewardSmoothedVelocityEstimate string `pg:",notnull"`
	TotalMinedReward                  string `pg:",notnull"`

	NewReward            string `pg:",use_zero"`
	EffectiveNetworkTime int64  `pg:",use_zero"`
}

func (r *ChainReward) PersistWithTx(ctx context.Context, tx *pg.Tx) error {
	ctx, span := global.Tracer("").Start(ctx, "ChainReward.PersistWithTx")
	defer span.End()

	stop := metrics.Timer(ctx, metrics.PersistDuration)
	defer stop()

	if _, err := tx.ModelContext(ctx, r).
		OnConflict("do nothing").
		Insert(); err != nil {
		return err
	}
	return nil
}

func (r *ChainReward) Persist(ctx context.Context, db *pg.DB) error {
	return db.RunInTransaction(ctx, func(tx *pg.Tx) error {
		return r.PersistWithTx(ctx, tx)
	})
}
