// Package rpcsource implements source.EventSource for standalone mode, where
// SoroLens polls Stellar RPC itself and serves events back out of its own
// Postgres database.
//
// The read path here is deliberately thin: it forwards to the store, which
// does the real work. The write path — the poller that fills that store — is
// in internal/ingest.
package rpcsource

import (
	"context"
	"fmt"

	"github.com/sorotrail/sorolens/internal/rpc"
	"github.com/sorotrail/sorolens/internal/source"
	"github.com/sorotrail/sorolens/internal/store"
)

// RetentionNote explains standalone mode's central limitation. Stellar RPC
// keeps contract events for roughly 24 hours to 7 days, so an instance can
// only ever capture what was emitted while it was running. The UI shows this
// so nobody mistakes an empty explorer for a broken one.
const RetentionNote = "Standalone mode captures events only while SoroLens is running: " +
	"Stellar RPC retains contract events for about 24 hours to 7 days, so history " +
	"from before this instance started is not available. Point SOURCE_MODE at a " +
	"SoroTrail indexer to browse deeper history."

// Source reads stored events. It satisfies source.EventSource.
type Source struct {
	store store.Store
	rpc   rpc.Client
}

// New returns a standalone-mode source. rpcClient is used only for status
// reporting; all reads come from the store.
func New(st store.Store, rpcClient rpc.Client) *Source {
	return &Source{store: st, rpc: rpcClient}
}

// ListContracts returns contracts ordered by most recent activity.
func (s *Source) ListContracts(ctx context.Context, q source.ContractQuery) (source.ContractPage, error) {
	contracts, next, err := s.store.ListContracts(ctx, q)
	if err != nil {
		return source.ContractPage{}, err
	}
	return source.ContractPage{Contracts: contracts, NextCursor: next}, nil
}

// ListEvents returns events newest first.
func (s *Source) ListEvents(ctx context.Context, q source.EventQuery) (source.EventPage, error) {
	events, next, err := s.store.QueryEvents(ctx, q)
	if err != nil {
		return source.EventPage{}, err
	}
	return source.EventPage{Events: events, NextCursor: next}, nil
}

// GetEvent returns one event by TOID.
func (s *Source) GetEvent(ctx context.Context, id string) (source.Event, error) {
	return s.store.GetEvent(ctx, id)
}

// ContractStats summarizes one contract's stored events.
func (s *Source) ContractStats(ctx context.Context, contractID string) (source.ContractStats, error) {
	return s.store.ContractStats(ctx, contractID)
}

// EventBuckets aggregates stored event counts into contiguous half-open
// [Start, End) time buckets. Standalone counts are exact.
func (s *Source) EventBuckets(ctx context.Context, q source.BucketQuery) (source.BucketSeries, error) {
	return s.store.EventBuckets(ctx, q)
}

// Stats summarizes everything stored. Standalone counts are exact.
func (s *Source) Stats(ctx context.Context) (source.Stats, error) {
	return s.store.Stats(ctx)
}

// Status reports database and RPC health. It never returns an error: an
// unreachable dependency becomes an unhealthy Status so the UI can always
// render its banner.
func (s *Source) Status(ctx context.Context) source.Status {
	st := source.Status{
		Mode:          string(sourceModeRPC),
		Healthy:       true,
		RetentionNote: RetentionNote,
	}

	if err := s.store.Ping(ctx); err != nil {
		st.Healthy = false
		st.Detail = fmt.Sprintf("database unreachable: %v", err)
		return st
	}

	health, err := s.rpc.GetHealth(ctx)
	switch {
	case err != nil:
		st.Healthy = false
		st.Detail = fmt.Sprintf("stellar rpc unreachable: %v", err)
	case health.Status != "healthy":
		st.Healthy = false
		st.Detail = fmt.Sprintf("stellar rpc reports %q", health.Status)
	default:
		st.LatestLedger = int64(health.LatestLedger)
	}
	return st
}

// sourceModeRPC mirrors config.ModeRPC. It is duplicated rather than imported
// to keep this package free of a dependency on config.
const sourceModeRPC = "rpc"
