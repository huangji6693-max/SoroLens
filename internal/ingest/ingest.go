// Package ingest polls Stellar RPC for contract events and writes them to the
// store. It runs only in standalone mode.
package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/sorotrail/sorolens/internal/decode"
	"github.com/sorotrail/sorolens/internal/metrics"
	"github.com/sorotrail/sorolens/internal/rpc"
	"github.com/sorotrail/sorolens/internal/source"
	"github.com/sorotrail/sorolens/internal/store"
)

// MaxPagesPerBatch bounds how many cursor pages one batch may follow in a
// single poll. Without it, a node that keeps returning full pages could hold
// the loop indefinitely and starve the shutdown path.
const MaxPagesPerBatch = 1000

// Options configure an Ingester.
type Options struct {
	// WatchedContracts limits ingestion to these contract IDs. Empty ingests
	// every contract event.
	WatchedContracts []string
	// PollInterval is how long to sleep once caught up to the chain tip.
	PollInterval time.Duration
	// StartLedger forces a cold start from this ledger. Zero reaches back
	// RetentionLedgers from the tip instead.
	StartLedger uint32
	// RetentionLedgers is the cold-start reach-back when StartLedger is zero.
	RetentionLedgers uint32
}

// EventPublisher receives newly ingested events after they have been persisted
// to the store. Implementations must not block the caller.
type EventPublisher interface {
	Publish(events []source.Event)
}

// Ingester polls the RPC and persists what it finds.
type Ingester struct {
	rpc       rpc.Client
	store     store.Store
	decoder   decode.Decoder
	opts      Options
	log       *slog.Logger
	// metrics is optional instrumentation; nil-safe, see internal/metrics.
	metrics   *metrics.Metrics
	publisher EventPublisher
}

// New builds an Ingester.
func New(client rpc.Client, st store.Store, dec decode.Decoder, opts Options, log *slog.Logger) *Ingester {
	if opts.PollInterval <= 0 {
		opts.PollInterval = 5 * time.Second
	}
	return &Ingester{rpc: client, store: st, decoder: dec, opts: opts, log: log}
}

// WithMetrics attaches Prometheus instrumentation to the ingest loop.
func (i *Ingester) WithMetrics(m *metrics.Metrics) *Ingester {
	i.metrics = m
	return i
}

// WithPublisher attaches a non-blocking live event publisher (such as the SSE
// Broker) to be notified after each batch is stored.
func (i *Ingester) WithPublisher(p EventPublisher) *Ingester {
	i.publisher = p
	return i
}

// Run polls until ctx is cancelled. A failed poll is logged and retried on the
// next tick rather than stopping ingestion, since RPC endpoints routinely
// return transient errors.
func (i *Ingester) Run(ctx context.Context) error {
	i.log.Info("ingester starting",
		"watched_contracts", len(i.opts.WatchedContracts),
		"poll_interval", i.opts.PollInterval)

	if len(i.opts.WatchedContracts) > rpc.MaxWatchedContracts {
		i.log.Warn("watching many contracts requires several requests per poll",
			"count", len(i.opts.WatchedContracts),
			"per_request", rpc.MaxWatchedContracts)
	}

	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			i.log.Info("ingester stopping")
			return nil
		case <-timer.C:
		}

		start := time.Now()
		err := i.pollOnce(ctx)
		i.metrics.RecordPoll(err == nil, time.Since(start))
		if err != nil {
			if ctx.Err() != nil {
				i.log.Info("ingester stopping")
				return nil
			}
			i.log.Error("poll failed", "error", err)
		}
		timer.Reset(i.opts.PollInterval)
	}
}

// pollOnce ingests everything from the resume point up to the current tip.
func (i *Ingester) pollOnce(ctx context.Context) error {
	latest, err := i.rpc.GetLatestLedger(ctx)
	if err != nil {
		return fmt.Errorf("fetching latest ledger: %w", err)
	}

	start, err := i.resumeLedger(ctx, latest.Sequence)
	if err != nil {
		return err
	}
	if start > latest.Sequence {
		// Already caught up; nothing has closed since the last poll.
		i.metrics.SetLag(0)
		return nil
	}
	i.metrics.SetLag(int64(latest.Sequence) - int64(start))

	// endLedger is exclusive, so +1 includes the tip itself.
	endExclusive := latest.Sequence + 1

	var total int64
	for _, batch := range filterBatches(i.opts.WatchedContracts) {
		n, err := i.ingestBatch(ctx, batch, start, endExclusive)
		total += n
		if err != nil {
			return err
		}
	}

	if err := i.store.SaveIngestState(ctx, store.IngestState{LastLedger: int64(latest.Sequence)}); err != nil {
		return err
	}

	i.metrics.RecordEvents(total)
	i.metrics.SetLag(0) // caught up: the resume point now equals the tip

	if total > 0 {
		i.log.Info("ingested events", "count", total, "from_ledger", start, "to_ledger", latest.Sequence)
	} else {
		i.log.Debug("no new events", "from_ledger", start, "to_ledger", latest.Sequence)
	}
	return nil
}

// ingestBatch follows the cursor for one filter batch until the stream drains.
func (i *Ingester) ingestBatch(ctx context.Context, filters []rpc.EventFilter, start, endExclusive uint32) (int64, error) {
	var (
		cursor   string
		inserted int64
	)

	for page := 0; page < MaxPagesPerBatch; page++ {
		if err := ctx.Err(); err != nil {
			return inserted, err
		}

		req := rpc.GetEventsRequest{
			Filters:    filters,
			Pagination: &rpc.Pagination{Limit: rpc.DefaultEventsLimit},
		}
		// startLedger and a cursor are mutually exclusive: the cursor already
		// encodes the position, and nodes reject requests carrying both.
		if cursor == "" {
			req.StartLedger = start
			req.EndLedger = endExclusive
		} else {
			req.Pagination.Cursor = cursor
		}

		res, err := i.rpc.GetEvents(ctx, req)
		if err != nil {
			return inserted, fmt.Errorf("fetching events: %w", err)
		}
		if len(res.Events) == 0 {
			return inserted, nil
		}

		events, err := i.convert(res.Events)
		if err != nil {
			return inserted, err
		}
		n, err := i.store.UpsertEvents(ctx, events)
		if err != nil {
			return inserted, err
		}
		inserted += n
		if i.publisher != nil && len(events) > 0 {
			i.publisher.Publish(events)
		}

		// A short page means the stream is drained for now.
		if len(res.Events) < rpc.DefaultEventsLimit {
			return inserted, nil
		}
		next := nextCursor(res)
		if next == "" || next == cursor {
			return inserted, nil
		}
		cursor = next
	}

	i.log.Warn("stopped following cursor at the page cap; will resume next poll",
		"max_pages", MaxPagesPerBatch)
	return inserted, nil
}

// convert decodes RPC events into the storage shape.
func (i *Ingester) convert(events []rpc.Event) ([]source.Event, error) {
	out := make([]source.Event, 0, len(events))
	for _, e := range events {
		topics, value, err := decode.TopicsValue(i.decoder, decode.RawEvent{
			Topic:     e.Topic,
			Value:     e.Value,
			TopicJSON: e.TopicJSON,
			ValueJSON: e.ValueJSON,
		})
		if err != nil {
			return nil, fmt.Errorf("decoding event %s: %w", e.ID, err)
		}
		out = append(out, source.Event{
			ID:               e.ID,
			ContractID:       e.ContractID,
			Ledger:           int64(e.Ledger),
			Type:             e.Type,
			TxHash:           e.TxHash,
			TxIndex:          e.TxIndex,
			OpIndex:          e.OpIndex,
			InSuccessfulCall: e.InSuccessfulContractCall,
			Topics:           topics,
			Value:            value,
			LedgerClosedAt:   e.LedgerClosedAt,
		})
	}
	return out, nil
}

// resumeLedger decides where this poll should start: just past whatever was
// ingested last, or a cold-start position when nothing has been ingested yet.
func (i *Ingester) resumeLedger(ctx context.Context, latest uint32) (uint32, error) {
	state, err := i.store.GetIngestState(ctx)
	if err != nil {
		return 0, err
	}
	if state.LastLedger > 0 {
		return uint32(state.LastLedger) + 1, nil
	}

	if i.opts.StartLedger > 0 {
		return i.opts.StartLedger, nil
	}

	// Cold start: reach back over the retention window, clamped at ledger 1.
	if latest > i.opts.RetentionLedgers {
		return latest - i.opts.RetentionLedgers, nil
	}
	return 1, nil
}

// nextCursor prefers the response's own cursor and falls back to the last
// event's paging token, which is what older nodes provide.
func nextCursor(res rpc.GetEventsResult) string {
	if res.Cursor != "" {
		return res.Cursor
	}
	if len(res.Events) > 0 {
		return res.Events[len(res.Events)-1].PagingToken
	}
	return ""
}

// filterBatches splits watched contracts into request-sized groups, honoring
// both the per-filter contract cap and the per-request filter cap. With no
// watched contracts it returns a single batch matching every contract event.
func filterBatches(contracts []string) [][]rpc.EventFilter {
	if len(contracts) == 0 {
		return [][]rpc.EventFilter{{{Type: "contract"}}}
	}

	var filters []rpc.EventFilter
	for start := 0; start < len(contracts); start += rpc.MaxContractIDsPerFilter {
		end := min(start+rpc.MaxContractIDsPerFilter, len(contracts))
		filters = append(filters, rpc.EventFilter{
			Type:        "contract",
			ContractIDs: contracts[start:end],
		})
	}

	var batches [][]rpc.EventFilter
	for start := 0; start < len(filters); start += rpc.MaxFiltersPerRequest {
		end := min(start+rpc.MaxFiltersPerRequest, len(filters))
		batches = append(batches, filters[start:end])
	}
	return batches
}

// ErrStopped signals a clean shutdown to callers that distinguish it from a
// genuine failure.
var ErrStopped = errors.New("ingester stopped")
