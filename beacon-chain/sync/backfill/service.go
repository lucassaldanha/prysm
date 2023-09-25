package backfill

import (
	"context"

	"github.com/pkg/errors"
	"github.com/prysmaticlabs/prysm/v4/beacon-chain/core/helpers"
	"github.com/prysmaticlabs/prysm/v4/beacon-chain/p2p"
	"github.com/prysmaticlabs/prysm/v4/beacon-chain/p2p/peers"
	"github.com/prysmaticlabs/prysm/v4/beacon-chain/startup"
	"github.com/prysmaticlabs/prysm/v4/config/params"
	"github.com/prysmaticlabs/prysm/v4/consensus-types/primitives"
	"github.com/prysmaticlabs/prysm/v4/encoding/bytesutil"
	"github.com/prysmaticlabs/prysm/v4/proto/dbval"
	"github.com/prysmaticlabs/prysm/v4/runtime"
	"github.com/prysmaticlabs/prysm/v4/time/slots"
	log "github.com/sirupsen/logrus"
)

const defaultWorkerCount = 5

// TODO use the correct beacon param for blocks by range size instead
const defaultBatchSize = 64

type Service struct {
	ctx           context.Context
	su            *StatusUpdater
	ms            minimumSlotter
	cw            startup.ClockWaiter
	nWorkers      int
	errChan       chan error
	batchSeq      *batchSequencer
	batchSize     uint64
	pool          BatchWorkerPool
	verifier      *verifier
	p2p           p2p.P2P
	batchImporter batchImporter
}

var _ runtime.Service = (*Service)(nil)

type ServiceOption func(*Service) error

func WithWorkerCount(n int) ServiceOption {
	return func(s *Service) error {
		s.nWorkers = n
		return nil
	}
}

func WithBatchSize(n uint64) ServiceOption {
	return func(s *Service) error {
		s.batchSize = n
		return nil
	}
}

type minimumSlotter interface {
	minimumSlot() primitives.Slot
	setClock(*startup.Clock)
}

type defaultMinimumSlotter struct {
	clock *startup.Clock
	cw    startup.ClockWaiter
	ctx   context.Context
}

func (d defaultMinimumSlotter) minimumSlot() primitives.Slot {
	if d.clock == nil {
		var err error
		d.clock, err = d.cw.WaitForClock(d.ctx)
		if err != nil {
			log.WithError(err).Fatal("failed to obtain system/genesis clock, unable to start backfill service")
		}
	}
	return MinimumBackfillSlot(d.clock.CurrentSlot())
}

func (d defaultMinimumSlotter) setClock(c *startup.Clock) {
	d.clock = c
}

var _ minimumSlotter = &defaultMinimumSlotter{}

type batchImporter func(ctx context.Context, b batch, su *StatusUpdater) (*dbval.BackfillStatus, error)

func defaultBatchImporter(ctx context.Context, b batch, su *StatusUpdater) (*dbval.BackfillStatus, error) {
	status := su.status()
	if err := b.ensureParent(bytesutil.ToBytes32(status.LowParentRoot)); err != nil {
		return status, err
	}
	// Import blocks to db and update db state to reflect the newly imported blocks.
	// Other parts of the beacon node may use the same StatusUpdater instance
	// via the coverage.AvailableBlocker interface to safely determine if a given slot has been backfilled.
	status, err := su.fillBack(ctx, b.results)
	if err != nil {
		log.WithError(err).Fatal("Non-recoverable db error in backfill service, quitting.")
	}
	return status, nil
}

func NewService(ctx context.Context, su *StatusUpdater, cw startup.ClockWaiter, p p2p.P2P, opts ...ServiceOption) (*Service, error) {
	s := &Service{
		ctx:           ctx,
		su:            su,
		cw:            cw,
		ms:            &defaultMinimumSlotter{cw: cw, ctx: ctx},
		p2p:           p,
		batchImporter: defaultBatchImporter,
	}
	for _, o := range opts {
		if err := o(s); err != nil {
			return nil, err
		}
	}
	if s.nWorkers == 0 {
		s.nWorkers = defaultWorkerCount
	}
	if s.batchSize == 0 {
		s.batchSize = defaultBatchSize
	}
	s.pool = newP2PBatchWorkerPool(p, s.nWorkers)

	return s, nil
}

func (s *Service) initVerifier(ctx context.Context) (*verifier, error) {
	cps, err := s.su.originState(ctx)
	if err != nil {
		return nil, err
	}
	return newBackfillVerifier(cps)
}

func (s *Service) updateComplete() bool {
	b, err := s.pool.Complete()
	if err != nil {
		if errors.Is(err, errEndSequence) {
			log.WithField("backfill_slot", b.begin).Info("Backfill is complete")
			return true
		}
		log.WithError(err).Fatal("Non-recoverable error in backfill service, quitting.")
		return false
	}
	s.batchSeq.update(b)
	return false
}

func (s *Service) importBatches(ctx context.Context) {
	importable := s.batchSeq.importable()
	imported := 0
	defer func() {
		if imported == 0 {
			return
		}
		backfillBatchesImported.Add(float64(imported))
	}()
	for i := range importable {
		ib := importable[i]
		// TODO: can we have an entire batch of skipped slots?
		if len(ib.results) == 0 {
			log.Error("wtf")
		}
		_, err := s.batchImporter(ctx, ib, s.su)
		if err != nil {
			log.WithError(err).WithFields(ib.logFields()).Debug("Backfill batch failed to import.")
			s.downscore(ib)
			s.batchSeq.update(ib.withState(batchErrRetryable))
			// If a batch fails, the subsequent batches are no longer considered importable.
			break
		}
		s.batchSeq.update(ib.withState(batchImportComplete))
		imported += 1
		// Calling update with state=batchImportComplete will advance the batch list.
	}
	log.WithField("imported", imported).WithField("importable", len(importable)).
		WithField("batches_remaining", s.batchSeq.numTodo()).
		Info("Backfill batches processed.")
}

func (s *Service) scheduleTodos() {
	batches, err := s.batchSeq.sequence()
	if err != nil {
		// This typically means we have several importable batches, but they are stuck behind a batch that needs
		// to complete first so that we can chain parent roots across batches.
		// ie backfilling [[90..100), [80..90), [70..80)], if we complete [70..80) and [80..90) but not [90..100),
		// we can't move forward until [90..100) completes, because we need to confirm 99 connects to 100,
		// and then we'll have the parent_root expected by 90 to ensure it matches the root for 89,
		// at which point we know we can process [80..90).
		if errors.Is(err, errMaxBatches) {
			log.Debug("Backfill batches waiting for descendent batch to complete.")
			return
		}
	}
	for _, b := range batches {
		s.pool.Todo(b)
	}
}

func (s *Service) Start() {
	ctx, cancel := context.WithCancel(s.ctx)
	defer func() {
		cancel()
	}()
	clock, err := s.cw.WaitForClock(ctx)
	if err != nil {
		log.WithError(err).Fatal("backfill service failed to start while waiting for genesis data")
	}
	s.ms.setClock(clock)

	status := s.su.status()
	s.batchSeq = newBatchSequencer(s.nWorkers, s.ms.minimumSlot(), primitives.Slot(status.LowSlot), primitives.Slot(s.batchSize))
	// Exit early if there aren't going to be any batches to backfill.
	if primitives.Slot(status.LowSlot) < s.ms.minimumSlot() {
		return
	}
	originE := slots.ToEpoch(primitives.Slot(status.OriginSlot))
	assigner := peers.NewAssigner(ctx, s.p2p.Peers(), params.BeaconConfig().MaxPeersToSync, originE)
	s.verifier, err = s.initVerifier(ctx)
	if err != nil {
		log.WithError(err).Fatal("Unable to initialize backfill verifier, quitting.")
	}
	s.pool.Spawn(ctx, s.nWorkers, clock, assigner, s.verifier)

	if err = s.initBatches(); err != nil {
		log.WithError(err).Fatal("Non-recoverable error in backfill service, quitting.")
	}

	for {
		if allComplete := s.updateComplete(); allComplete {
			return
		}
		s.importBatches(ctx)
		batchesWaiting.Set(float64(s.batchSeq.countWithState(batchImportable)))
		if err := s.batchSeq.moveMinimum(s.ms.minimumSlot()); err != nil {
			log.WithError(err).Fatal("Non-recoverable error in backfill service, quitting.")
		}
		s.scheduleTodos()
	}
}

func (s *Service) initBatches() error {
	batches, err := s.batchSeq.sequence()
	if err != nil {
		return err
	}
	for _, b := range batches {
		s.pool.Todo(b)
	}
	return nil
}

func (s *Service) downscore(b batch) {
}

func (s *Service) Stop() error {
	return nil
}

func (s *Service) Status() error {
	return nil
}

// MinimumBackfillSlot determines the lowest slot that backfill needs to download based on looking back
// MIN_EPOCHS_FOR_BLOCK_REQUESTS from the current slot.
func MinimumBackfillSlot(current primitives.Slot) primitives.Slot {
	oe := helpers.MinEpochsForBlockRequests()
	if oe > slots.MaxSafeEpoch() {
		oe = slots.MaxSafeEpoch()
	}
	offset := slots.UnsafeEpochStart(oe)
	if offset > current {
		return 0
	}
	return current - offset
}
