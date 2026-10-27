// Package store persists contract events and ingestion state in Postgres. It
// backs standalone mode only; upstream mode reads from SoroTrail and never
// opens a database.
package store

import (
	"context"
	"time"

	"github.com/sorotrail/sorolens/internal/source"
)

// Paging bounds for store queries, mirroring the source package so a limit
// means the same thing at every layer.
const (
	DefaultQueryLimit = source.DefaultLimit
	MaxQueryLimit     = source.MaxLimit
)

// IngestState records how far the ingester has progressed, so a restart
// resumes instead of re-reading from the retention horizon.
type IngestState struct {
	LastLedger int64
	LastCursor string
	UpdatedAt  time.Time
}

// Store is the persistence boundary. The ingester and the standalone
// EventSource depend on this interface, never on Postgres directly.
//
// contributors: an alternative backend (SQLite for single-binary deploys, say)
// only needs to implement this interface and be wired into cmd/sorolens.
type Store interface {
	// UpsertEvents inserts events idempotently, keyed on ID, and returns how
	// many rows were newly inserted. Re-reading a ledger range must never
	// duplicate rows.
	UpsertEvents(ctx context.Context, events []source.Event) (int64, error)
	// GetEvent returns one event by ID, or source.ErrNotFound.
	GetEvent(ctx context.Context, id string) (source.Event, error)
	// QueryEvents returns a page of events newest first, plus a cursor for the
	// next page ("" when the results are exhausted).
	QueryEvents(ctx context.Context, q source.EventQuery) ([]source.Event, string, error)
	// ListContracts returns aggregated per-contract rows, most recently active
	// first, plus a next-page cursor.
	ListContracts(ctx context.Context, q source.ContractQuery) ([]source.Contract, string, error)
	// ContractStats summarizes one contract's stored events.
	ContractStats(ctx context.Context, contractID string) (source.ContractStats, error)
	// EventBuckets aggregates stored event counts into contiguous half-open
	// [Start, End) time buckets over [q.From, q.To) by ledger_closed_at.
	EventBuckets(ctx context.Context, q source.BucketQuery) (source.BucketSeries, error)
	// Stats summarizes everything stored.
	Stats(ctx context.Context) (source.Stats, error)

	GetIngestState(ctx context.Context) (IngestState, error)
	SaveIngestState(ctx context.Context, s IngestState) error

	Ping(ctx context.Context) error
	Close()
}
