// Package source defines EventSource, the single seam between SoroLens's
// read paths (the JSON API and the web UI) and wherever contract events
// actually come from.
//
// Two implementations ship with SoroLens, selected by SOURCE_MODE:
//
//   - rpcsource: standalone mode. SoroLens polls Stellar RPC itself and reads
//     back from its own Postgres database.
//   - sorotrailsource: upstream mode. SoroLens reads from a SoroTrail
//     indexer's HTTP API and keeps no database, so it can show history the RPC
//     has already dropped.
//
// contributors: to add a new backend, implement EventSource in a new package
// under internal/source and wire it into the switch in cmd/sorolens. Nothing
// outside that switch should need to change — the API and web layers only ever
// see this interface.
//
// This package deliberately has no dependencies beyond the standard library,
// so both the storage layer and every implementation can import it freely.
package source

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ErrNotFound is returned by GetEvent when no event has the requested ID.
// Implementations must wrap or return this exact error so handlers can map it
// to a 404 without knowing which backend produced it.
var ErrNotFound = errors.New("not found")

// Paging and aggregation limits shared by every implementation, so bounds mean
// the same thing regardless of which backend serves the request.
const (
	DefaultLimit      = 50
	MaxLimit          = 200
	MaxBuckets        = 500
	MinBucketInterval = time.Minute
	MaxBucketInterval = 7 * 24 * time.Hour
)

// Event is one Soroban contract event.
type Event struct {
	// ID is the RPC's TOID-based identifier. IDs are zero-padded, so their
	// lexicographic order matches chronological order — cursor pagination
	// throughout SoroLens relies on this.
	ID               string `json:"id"`
	ContractID       string `json:"contract_id"`
	Ledger           int64  `json:"ledger"`
	Type             string `json:"type"`
	TxHash           string `json:"tx_hash"`
	TxIndex          int32  `json:"tx_index"`
	OpIndex          int32  `json:"op_index"`
	InSuccessfulCall bool   `json:"in_successful_call"`
	// Topics is a JSON array of decoded ScVals; Value is a single decoded
	// ScVal. Both use the RPC's single-key wrapper vocabulary, e.g.
	// {"symbol":"transfer"}. Use the decode package to render them.
	Topics json.RawMessage `json:"topics"`
	Value  json.RawMessage `json:"value"`
	// LedgerClosedAt is when the event's ledger closed — the on-chain time.
	LedgerClosedAt time.Time `json:"ledger_closed_at"`
	// CreatedAt is when this row was ingested, which in standalone mode is
	// unrelated to when the event happened.
	CreatedAt time.Time `json:"created_at"`
}

// Contract summarizes every event SoroLens holds for one contract.
type Contract struct {
	ID          string `json:"id"`
	EventCount  int64  `json:"event_count"`
	FirstLedger int64  `json:"first_ledger"`
	LastLedger  int64  `json:"last_ledger"`
	// LastActivity is the close time of the most recent ledger in which this
	// contract emitted an event.
	LastActivity time.Time `json:"last_activity"`
}

// EventQuery narrows a ListEvents call. Zero values mean "no constraint".
type EventQuery struct {
	ContractID string
	Type       string
	// TxHash restricts the query to events produced by one transaction —
	// the natural follow-up after seeing an interesting event in a list.
	TxHash string
	// Topic matches events having this exact decoded JSON value at any topic
	// position, e.g. {"symbol":"transfer"}.
	Topic      json.RawMessage
	FromLedger int64 // inclusive
	ToLedger   int64 // inclusive
	// Cursor continues a previous page. It is opaque: only the implementation
	// that produced it may interpret it.
	Cursor string
	Limit  int
}

// ContractQuery narrows a ListContracts call.
type ContractQuery struct {
	// Search matches contract IDs containing this substring, case-insensitive.
	Search string
	Cursor string
	Limit  int
}

// EventPage is one page of events, newest first.
type EventPage struct {
	Events []Event `json:"events"`
	// NextCursor is non-empty when more results exist.
	NextCursor string `json:"next_cursor,omitempty"`
}

// ContractPage is one page of contracts, most recently active first.
type ContractPage struct {
	Contracts  []Contract `json:"contracts"`
	NextCursor string     `json:"next_cursor,omitempty"`
}

// TypeCount is one bucket of an event-type breakdown.
type TypeCount struct {
	Type  string `json:"type"`
	Count int64  `json:"count"`
}

// Stats summarizes what this source can currently see.
type Stats struct {
	TotalEvents   int64 `json:"total_events"`
	ContractCount int64 `json:"contract_count"`
	// FirstLedger and LastLedger bound the range covered. Zero means unknown,
	// which upstream mode reports when it cannot determine the range cheaply.
	FirstLedger int64 `json:"first_ledger"`
	LastLedger  int64 `json:"last_ledger"`
	// TypeBreakdown counts events per type across everything visible.
	TypeBreakdown []TypeCount `json:"type_breakdown,omitempty"`
	// Approximate is true when the numbers are derived from a bounded scan
	// rather than counted exactly. Upstream mode sets this; the UI must label
	// such figures so nobody reads them as authoritative.
	Approximate bool `json:"approximate"`
}

// ContractStats summarizes one contract's events.
type ContractStats struct {
	ContractID    string      `json:"contract_id"`
	TotalEvents   int64       `json:"total_events"`
	FirstLedger   int64       `json:"first_ledger"`
	LastLedger    int64       `json:"last_ledger"`
	TypeBreakdown []TypeCount `json:"type_breakdown,omitempty"`
	Approximate   bool        `json:"approximate"`
}

// BucketQuery specifies a time-bucketed event count aggregation over the
// half-open time range [From, To), partitioned into contiguous half-open
// [Start, End) intervals of width Interval anchored at From.
//
// Events are bucketed by their on-chain ledger close timestamp
// (ledger_closed_at), not the local ingestion timestamp (created_at), so
// backfilled or delayed ingestion never distorts the activity timeline.
type BucketQuery struct {
	ContractID string
	Type       string
	Topic      json.RawMessage
	From       time.Time
	To         time.Time
	Interval   time.Duration
}

// EventBucket is one half-open time slice [Start, End) and its event count.
type EventBucket struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
	Count int64     `json:"count"`
}

// BucketSeries is the response payload for time-bucketed event counts.
type BucketSeries struct {
	Interval    string        `json:"interval"`
	From        time.Time     `json:"from"`
	To          time.Time     `json:"to"`
	TotalEvents int64         `json:"total_events"`
	Buckets     []EventBucket `json:"buckets"`
	// Approximate is true when the buckets were derived from a bounded scan in
	// upstream mode rather than an exact SQL aggregate.
	Approximate bool `json:"approximate"`
}

// Status describes a source's health and the extent of the history it holds.
// The UI shows this prominently: standalone mode only sees events emitted
// while it was running, which surprises people who expect a full explorer.
type Status struct {
	// Mode is the configured SOURCE_MODE, "rpc" or "sorotrail".
	Mode string `json:"mode"`
	// Healthy is false when the backend is unreachable or degraded.
	Healthy bool `json:"healthy"`
	// Detail explains an unhealthy status, or adds context to a healthy one.
	Detail string `json:"detail,omitempty"`
	// LatestLedger is the newest ledger the backend knows about, if known.
	LatestLedger int64 `json:"latest_ledger,omitempty"`
	// RetentionNote is a human-readable caveat about history coverage, shown
	// in the UI banner.
	RetentionNote string `json:"retention_note,omitempty"`
}

// EventSource is the read-only interface every backend implements. All methods
// must be safe for concurrent use by multiple HTTP handlers.
type EventSource interface {
	// ListContracts returns contracts ordered by most recent activity.
	ListContracts(ctx context.Context, q ContractQuery) (ContractPage, error)
	// ListEvents returns events newest first.
	ListEvents(ctx context.Context, q EventQuery) (EventPage, error)
	// GetEvent returns one event by TOID, or ErrNotFound.
	GetEvent(ctx context.Context, id string) (Event, error)
	// ContractStats summarizes one contract.
	ContractStats(ctx context.Context, contractID string) (ContractStats, error)
	// EventBuckets aggregates event counts into contiguous half-open [Start, End)
	// time buckets over [q.From, q.To) by on-chain ledger_closed_at time.
	EventBuckets(ctx context.Context, q BucketQuery) (BucketSeries, error)
	// Stats summarizes everything visible to this source.
	Stats(ctx context.Context) (Stats, error)
	// Status reports health and history coverage. It must not return an error:
	// an unreachable backend is a Status with Healthy false, so the UI can
	// always render a banner.
	Status(ctx context.Context) Status
}

// NormalizeLimit clamps a requested page size into the shared bounds.
func NormalizeLimit(limit int) int {
	switch {
	case limit <= 0:
		return DefaultLimit
	case limit > MaxLimit:
		return MaxLimit
	default:
		return limit
	}
}

// FormatBucketInterval formats a bucket duration using its cleanest whole unit
// (e.g. "15m", "1h") rather than time.Duration.String()'s trailing zeroes.
func FormatBucketInterval(d time.Duration) string {
	switch {
	case d <= 0:
		return "0s"
	case d%time.Hour == 0:
		return fmt.Sprintf("%dh", int64(d/time.Hour))
	case d%time.Minute == 0:
		return fmt.Sprintf("%dm", int64(d/time.Minute))
	case d%time.Second == 0:
		return fmt.Sprintf("%ds", int64(d/time.Second))
	default:
		return d.String()
	}
}

// BuildEmptyBuckets constructs the contiguous sequence of half-open [Start, End)
// buckets covering [from, to) in steps of interval. Empty ranges (from >= to or
// interval <= 0) return an empty non-nil slice.
func BuildEmptyBuckets(from, to time.Time, interval time.Duration) []EventBucket {
	from = from.UTC()
	to = to.UTC()
	if !from.Before(to) || interval <= 0 {
		return []EventBucket{}
	}
	span := to.Sub(from)
	n := int(span / interval)
	if span%interval != 0 {
		n++
	}
	buckets := make([]EventBucket, 0, n)
	for start := from; start.Before(to); start = start.Add(interval) {
		end := start.Add(interval)
		if end.After(to) {
			end = to
		}
		buckets = append(buckets, EventBucket{
			Start: start,
			End:   end,
			Count: 0,
		})
	}
	return buckets
}

// CountEventsIntoBuckets tallies events whose on-chain LedgerClosedAt timestamp
// (falling back to CreatedAt when LedgerClosedAt is zero) lies within the
// half-open range [from, to) into contiguous [Start, End) buckets of width
// interval. Events with t == bucket.Start land in that bucket; events with
// t == bucket.End land in the next bucket (or are excluded when bucket.End == to).
func CountEventsIntoBuckets(from, to time.Time, interval time.Duration, events []Event) ([]EventBucket, int64) {
	from = from.UTC()
	to = to.UTC()
	buckets := BuildEmptyBuckets(from, to, interval)
	if len(buckets) == 0 {
		return buckets, 0
	}
	var total int64
	for _, e := range events {
		ts := e.LedgerClosedAt
		if ts.IsZero() {
			ts = e.CreatedAt
		}
		ts = ts.UTC()
		if ts.Before(from) || !ts.Before(to) {
			continue
		}
		idx := int(ts.Sub(from) / interval)
		if idx >= 0 && idx < len(buckets) {
			buckets[idx].Count++
			total++
		}
	}
	return buckets, total
}
