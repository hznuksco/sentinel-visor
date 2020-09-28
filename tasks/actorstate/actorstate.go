package actorstate

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"go.opentelemetry.io/otel/api/global"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/sentinel-visor/lens"
	"github.com/filecoin-project/sentinel-visor/model"
	"github.com/filecoin-project/sentinel-visor/model/visor"
	"github.com/filecoin-project/sentinel-visor/storage"
	"github.com/filecoin-project/sentinel-visor/wait"
)

var log = logging.Logger("actorstate")

var timeNow = time.Now

const batchInterval = 100 * time.Millisecond // time to wait between batches

type ActorInfo struct {
	Actor           types.Actor
	Address         address.Address
	ParentStateRoot cid.Cid
	TipSet          types.TipSetKey
	ParentTipSet    types.TipSetKey
}

// An ActorStateExtracter extracts actor state into a persistable format
type ActorStateExtracter interface {
	Extract(ctx context.Context, a ActorInfo, node lens.API) (model.Persistable, error)
}

// All supported actor state extracters
var (
	extractersMu sync.Mutex
	extracters   = map[cid.Cid]ActorStateExtracter{}
)

// Register adds an actor state extracter
func Register(code cid.Cid, e ActorStateExtracter) {
	extractersMu.Lock()
	defer extractersMu.Unlock()
	if _, ok := extracters[code]; ok {
		log.Warningf("extracter overrides previously registered extracter for code %q", code.String())
	}
	extracters[code] = e
}

func SupportedActorCodes() []cid.Cid {
	extractersMu.Lock()
	defer extractersMu.Unlock()

	var codes []cid.Cid
	for code := range extracters {
		codes = append(codes, code)
	}
	return codes
}

func NewActorStateProcessor(d *storage.Database, node lens.API, leaseLength time.Duration, batchSize int, maxHeight int64, actorCodes []cid.Cid) (*ActorStateProcessor, error) {
	p := &ActorStateProcessor{
		node:        node,
		storage:     d,
		leaseLength: leaseLength,
		batchSize:   batchSize,
		maxHeight:   maxHeight,
		extracters:  map[cid.Cid]ActorStateExtracter{},
	}

	extractersMu.Lock()
	defer extractersMu.Unlock()
	for _, code := range actorCodes {
		e, exists := extracters[code]
		if !exists {
			return nil, xerrors.Errorf("unsupport actor code: %s", code.String())
		}
		p.actorCodes = append(p.actorCodes, code.String())
		p.extracters[code] = e
	}

	return p, nil
}

// ActorStateProcessor is a task that processes actor state changes and persists them to the database.
// There will be multiple concurrent ActorStateProcessor instances.
type ActorStateProcessor struct {
	node        lens.API
	storage     *storage.Database
	leaseLength time.Duration                   // length of time to lease work for
	batchSize   int                             // number of blocks to lease in a batch
	maxHeight   int64                           // limit processing to tipsets equal to or below this height
	actorCodes  []string                        // list of actor codes that will be requested
	extracters  map[cid.Cid]ActorStateExtracter // list of extracters that will be used
}

// Run starts processing batches of actors and blocks until the context is done or
// an error occurs.
func (p *ActorStateProcessor) Run(ctx context.Context) error {
	// Loop until context is done or processing encounters a fatal error
	return wait.RepeatUntil(ctx, batchInterval, p.processBatch)
}

func (p *ActorStateProcessor) processBatch(ctx context.Context) (bool, error) {
	ctx, span := global.Tracer("").Start(ctx, "ActorStateProcessor.processBatch")
	defer span.End()

	// Lease some blocks to work on
	claimUntil := timeNow().Add(p.leaseLength)
	ctx, cancel := context.WithDeadline(ctx, claimUntil)
	defer cancel()

	batch, err := p.storage.LeaseActors(ctx, claimUntil, p.batchSize, p.maxHeight, p.actorCodes)
	if err != nil {
		return true, err
	}

	// If we have no tipsets to work on then wait before trying again
	if len(batch) == 0 {
		sleepInterval := wait.Jitter(idleSleepInterval, 2)
		log.Debugf("no actors to process, waiting for %s", sleepInterval)
		time.Sleep(sleepInterval)
		return false, nil
	}

	log.Debugw("leased batch of actors", "count", len(batch))

	for _, actor := range batch {
		// Stop processing if we have somehow passed our own lease time
		select {
		case <-ctx.Done():
			return false, nil // Don't propagate cancelation error so we can resume processing cleanly
		default:
		}

		info, err := NewActorInfo(actor)
		if err != nil {
			log.Errorw("unmarshal actor", "error", err.Error())
			if err := p.storage.MarkActorComplete(ctx, actor.Head, actor.Code, timeNow(), err.Error()); err != nil {
				log.Errorw("failed to mark actor complete", "error", err.Error())
			}
			continue
		}

		var errorsEncountered string
		if err := p.processActor(ctx, info); err != nil {
			log.Errorw("process actor", "error", err.Error())
			errorsEncountered = err.Error()
		}

		if err := p.storage.MarkActorComplete(ctx, actor.Head, actor.Code, timeNow(), errorsEncountered); err != nil {
			log.Errorw("failed to mark actor complete", "error", err.Error())
		}
	}

	return false, nil
}

func (p *ActorStateProcessor) processActor(ctx context.Context, info ActorInfo) error {
	ctx, span := global.Tracer("").Start(ctx, "ActorStateProcessor.processActor")
	defer span.End()

	var ae ActorExtracter

	// Persist the raw state
	data, err := ae.Extract(ctx, info, p.node)
	if err != nil {
		return xerrors.Errorf("extract actor state: %w", err)
	}
	if err := data.Persist(ctx, p.storage.DB); err != nil {
		// TODO handle this case with a retry
		return xerrors.Errorf("persisting raw state: %w", err)
	}

	// Find a specific extracter for the actor type
	extracter, exists := p.extracters[info.Actor.Code]
	if !exists {
		return xerrors.Errorf("no extractor defined for actor code %q", info.Actor.Code.String())
	}

	data, err = extracter.Extract(ctx, info, p.node)
	if err != nil {
		return xerrors.Errorf("extract actor state: %w", err)
	}

	log.Debugw("persisting extracted state", "addr", info.Address.String())

	if err := data.Persist(ctx, p.storage.DB); err != nil {
		return xerrors.Errorf("persisting extracted state: %w", err)
	}

	return nil
}

func NewActorInfo(a *visor.ProcessingActor) (ActorInfo, error) {
	var info ActorInfo

	var err error
	info.TipSet, err = a.TipSetKey()
	if err != nil {
		return ActorInfo{}, xerrors.Errorf("unmarshal tipset: %w", err)
	}

	info.ParentTipSet, err = a.ParentTipSetKey()
	if err != nil {
		return ActorInfo{}, xerrors.Errorf("unmarshal parent tipset: %w", err)
	}

	info.ParentStateRoot, err = cid.Decode(a.ParentStateRoot)
	if err != nil {
		return ActorInfo{}, xerrors.Errorf("decode parent stateroot cid: %w", err)
	}

	info.Address, err = address.NewFromString(a.Address)
	if err != nil {
		return ActorInfo{}, xerrors.Errorf("address: %w", err)
	}

	info.Actor.Code, err = cid.Decode(a.Code)
	if err != nil {
		return ActorInfo{}, xerrors.Errorf("decode code cid: %w", err)
	}

	info.Actor.Head, err = cid.Decode(a.Head)
	if err != nil {
		return ActorInfo{}, xerrors.Errorf("decode head cid: %w", err)
	}

	info.Actor.Balance, err = big.FromString(a.Balance)
	if err != nil {
		return ActorInfo{}, xerrors.Errorf("parse balance: %w", err)
	}

	info.Actor.Nonce, err = strconv.ParseUint(a.Nonce, 10, 64)
	if err != nil {
		return ActorInfo{}, xerrors.Errorf("parse nonce: %w", err)
	}

	return info, nil
}