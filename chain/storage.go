package chain

import (
	"context"

	"github.com/filecoin-project/sentinel-visor/model"
)

type Storage interface {
	Persist(ctx context.Context, p model.PersistableWithTx) error
}

var _ Storage = (*NullStorage)(nil)

type NullStorage struct {
}

func (*NullStorage) Persist(ctx context.Context, p model.PersistableWithTx) error {
	log.Debugw("Not persisting data")
	return nil
}
