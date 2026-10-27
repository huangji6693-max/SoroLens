package sorotrailsource

import (
	"context"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sorotrail/sorolens/internal/source"
)

// RetentionNote explains what upstream mode can see. Coverage is whatever the
// SoroTrail instance has indexed, which is normally far deeper than the RPC's
// own window.
const RetentionNote = "Upstream mode reads from a SoroTrail indexer, so history reaches " +
	"as far back as that indexer has stored — well beyond Stellar RPC's own retention window. " +
	"Contract listings and type breakdowns are derived from a bounded scan of recent events " +
	"and are marked approximate."

// contractCacheTTL is how long a derived contracts list is reused. Deriving it
// costs several upstream requests, and the list changes slowly, so a short TTL
// keeps browsing responsive without serving stale data.
const contractCacheTTL = 30 * time.Second

// contractScanBudget is how many recent events one derivation reads before
// aggregating. It bounds both time and memory for the contracts page.
const contractScanBudget = 5000

// Source reads events from a SoroTrail indexer. It satisfies
// source.EventSource and is safe for concurrent use.
type Source struct {
	api apiClient

	mu           sync.Mutex
	cachedList   []source.Contract
	cachedAt     time.Time
	cachedLedger int64
}

// New returns an upstream-mode source reading from the SoroTrail instance at
// baseURL. Pass nil for httpClient to use a sensible default.
func New(baseURL string, httpClient *http.Client) *Source {
	return &Source{api: newHTTPAPI(strings.TrimRight(baseURL, "/"), httpClient)}
}

// ListEvents returns events newest first, reversing SoroTrail's ascending
// order through a backwards ledger scan.
func (s *Source) ListEvents(ctx context.Context, q source.EventQuery) (source.EventPage, error) {
	limit := source.NormalizeLimit(q.Limit)

	upstream := upstreamQuery{
		ContractID: q.ContractID,
		Type:       q.Type,
		TxHash:     q.TxHash,
		Topic:      q.Topic,
	}

	// Establish the newest ledger to scan down from: the caller's bound, the
	// cursor's resume point, or the indexer's tip.
	toLedger := q.ToLedger
	beforeID := ""
	if q.Cursor != "" {
		id, resume, err := decodeCursor(q.Cursor)
		if err != nil {
			return source.EventPage{}, err
		}
		beforeID = id
		if toLedger == 0 || resume < toLedger {
			toLedger = resume
		}
	}
	if toLedger == 0 {
		stats, err := s.api.Stats(ctx)
		if err != nil {
			return source.EventPage{}, err
		}
		toLedger = stats.LastIngestedLedger
		if toLedger == 0 {
			// The indexer has nothing yet.
			return source.EventPage{Events: []source.Event{}}, nil
		}
	}

	// Fetch one more than needed so the presence of a next page is known
	// without a second scan.
	res, err := s.scanBackwards(ctx, upstream, beforeID, limit+1, toLedger, q.FromLedger)
	if err != nil {
		return source.EventPage{}, err
	}

	events := res.events
	var next string
	if len(events) > limit {
		events = events[:limit]
		last := events[len(events)-1]
		next = encodeCursor(last.ID, last.Ledger)
	} else if !res.exhausted && len(events) > 0 {
		// The scan stopped on its window budget rather than on the end of
		// history, so older events may still exist. Offer a cursor that
		// resumes below the oldest event seen.
		last := events[len(events)-1]
		next = encodeCursor(last.ID, last.Ledger)
	}

	if events == nil {
		events = []source.Event{}
	}
	return source.EventPage{Events: events, NextCursor: next}, nil
}

// GetEvent fetches one event by TOID.
func (s *Source) GetEvent(ctx context.Context, id string) (source.Event, error) {
	return s.api.Event(ctx, id)
}

// ListContracts derives the contract list from a bounded scan of recent
// events, because SoroTrail exposes no contracts endpoint. Results are cached
// briefly and always reported as covering only the scanned window.
func (s *Source) ListContracts(ctx context.Context, q source.ContractQuery) (source.ContractPage, error) {
	limit := source.NormalizeLimit(q.Limit)

	all, err := s.contractList(ctx)
	if err != nil {
		return source.ContractPage{}, err
	}

	// Filter and paginate in memory: the derived list is bounded by
	// contractScanBudget, so it is small enough to handle here.
	filtered := all
	if q.Search != "" {
		needle := strings.ToUpper(q.Search)
		filtered = make([]source.Contract, 0, len(all))
		for _, c := range all {
			if strings.Contains(strings.ToUpper(c.ID), needle) {
				filtered = append(filtered, c)
			}
		}
	}

	start := 0
	if q.Cursor != "" {
		// Upstream contract cursors are a plain offset into the derived list.
		_, offset, err := decodeCursor(q.Cursor)
		if err != nil {
			return source.ContractPage{}, err
		}
		start = int(offset)
		if start > len(filtered) {
			start = len(filtered)
		}
	}

	end := min(start+limit, len(filtered))
	page := filtered[start:end]

	var next string
	if end < len(filtered) {
		next = encodeCursor("", int64(end))
	}

	if page == nil {
		page = []source.Contract{}
	}
	return source.ContractPage{Contracts: page, NextCursor: next}, nil
}

// contractList returns the cached derived contract list, refreshing it when
// stale.
func (s *Source) contractList(ctx context.Context) ([]source.Contract, error) {
	s.mu.Lock()
	if time.Since(s.cachedAt) < contractCacheTTL && s.cachedList != nil {
		cached := s.cachedList
		s.mu.Unlock()
		return cached, nil
	}
	s.mu.Unlock()

	stats, err := s.api.Stats(ctx)
	if err != nil {
		return nil, err
	}
	if stats.LastIngestedLedger == 0 {
		return []source.Contract{}, nil
	}

	res, err := s.scanBackwards(ctx, upstreamQuery{}, "", contractScanBudget, stats.LastIngestedLedger, 0)
	if err != nil {
		return nil, err
	}

	contracts := aggregateContracts(res.events)

	s.mu.Lock()
	s.cachedList = contracts
	s.cachedAt = time.Now()
	s.cachedLedger = stats.LastIngestedLedger
	s.mu.Unlock()

	return contracts, nil
}

// aggregateContracts rolls events up per contract, most recently active first.
func aggregateContracts(events []source.Event) []source.Contract {
	byID := make(map[string]*source.Contract)
	for _, e := range events {
		c, ok := byID[e.ContractID]
		if !ok {
			byID[e.ContractID] = &source.Contract{
				ID:           e.ContractID,
				EventCount:   1,
				FirstLedger:  e.Ledger,
				LastLedger:   e.Ledger,
				LastActivity: e.LedgerClosedAt,
			}
			continue
		}
		c.EventCount++
		if e.Ledger < c.FirstLedger {
			c.FirstLedger = e.Ledger
		}
		if e.Ledger > c.LastLedger {
			c.LastLedger = e.Ledger
			c.LastActivity = e.LedgerClosedAt
		}
	}

	out := make([]source.Contract, 0, len(byID))
	for _, c := range byID {
		out = append(out, *c)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].LastLedger != out[j].LastLedger {
			return out[i].LastLedger > out[j].LastLedger
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// ContractStats summarizes one contract from a bounded scan, so the figures
// are approximate by construction.
func (s *Source) ContractStats(ctx context.Context, contractID string) (source.ContractStats, error) {
	out := source.ContractStats{ContractID: contractID, Approximate: true}

	stats, err := s.api.Stats(ctx)
	if err != nil {
		return out, err
	}
	if stats.LastIngestedLedger == 0 {
		return out, nil
	}

	res, err := s.scanBackwards(ctx, upstreamQuery{ContractID: contractID}, "", contractScanBudget, stats.LastIngestedLedger, 0)
	if err != nil {
		return out, err
	}
	if len(res.events) == 0 {
		return out, nil
	}

	out.TotalEvents = int64(len(res.events))
	out.FirstLedger = res.events[len(res.events)-1].Ledger
	out.LastLedger = res.events[0].Ledger
	out.TypeBreakdown = typeBreakdown(res.events)
	// A scan that reached the end of history counted every event exactly.
	out.Approximate = !res.exhausted
	return out, nil
}

// EventBuckets derives time-bucketed event counts from a bounded backwards scan
// of upstream events. Like ContractStats, the series is flagged Approximate
// whenever the bounded scan did not exhaust upstream history, so a caller never
// mistakes a bounded scan for a complete dataset.
func (s *Source) EventBuckets(ctx context.Context, q source.BucketQuery) (source.BucketSeries, error) {
	from := q.From.UTC()
	to := q.To.UTC()
	out := source.BucketSeries{
		Interval:    source.FormatBucketInterval(q.Interval),
		From:        from,
		To:          to,
		Buckets:     source.BuildEmptyBuckets(from, to, q.Interval),
		Approximate: true,
	}
	if !from.Before(to) || q.Interval <= 0 {
		out.Approximate = false
		return out, nil
	}

	stats, err := s.api.Stats(ctx)
	if err != nil {
		return out, err
	}
	if stats.LastIngestedLedger == 0 {
		out.Approximate = false
		return out, nil
	}

	res, err := s.scanBackwards(ctx, upstreamQuery{
		ContractID: q.ContractID,
		Type:       q.Type,
		Topic:      q.Topic,
	}, "", contractScanBudget, stats.LastIngestedLedger, 0)
	if err != nil {
		return out, err
	}

	out.Buckets, out.TotalEvents = source.CountEventsIntoBuckets(from, to, q.Interval, res.events)
	out.Approximate = !res.exhausted
	return out, nil
}

// Stats combines SoroTrail's exact totals with a derived type breakdown.
func (s *Source) Stats(ctx context.Context) (source.Stats, error) {
	upstreamStats, err := s.api.Stats(ctx)
	if err != nil {
		return source.Stats{}, err
	}

	out := source.Stats{
		TotalEvents:   upstreamStats.TotalEvents,
		ContractCount: upstreamStats.ContractCount,
		LastLedger:    upstreamStats.LastIngestedLedger,
		// Totals above come straight from SoroTrail and are exact; the type
		// breakdown below is derived from a bounded scan, so the whole struct
		// is flagged approximate rather than implying the breakdown is exact.
		Approximate: true,
	}

	if upstreamStats.LastIngestedLedger > 0 {
		res, err := s.scanBackwards(ctx, upstreamQuery{}, "", contractScanBudget, upstreamStats.LastIngestedLedger, 0)
		if err == nil && len(res.events) > 0 {
			out.TypeBreakdown = typeBreakdown(res.events)
			out.FirstLedger = res.events[len(res.events)-1].Ledger
		}
	}
	return out, nil
}

// Status reports the upstream indexer's health.
func (s *Source) Status(ctx context.Context) source.Status {
	st := source.Status{
		Mode:          "sorotrail",
		Healthy:       true,
		RetentionNote: RetentionNote,
	}

	health, err := s.api.Health(ctx)
	if err != nil {
		st.Healthy = false
		st.Detail = "sorotrail unreachable: " + err.Error()
		return st
	}
	if health.Status != "ok" {
		st.Healthy = false
		st.Detail = "sorotrail reports " + health.Status
		for name, detail := range health.Checks {
			if detail != "ok" {
				st.Detail += "; " + name + ": " + detail
			}
		}
		return st
	}

	if stats, err := s.api.Stats(ctx); err == nil {
		st.LatestLedger = stats.LastIngestedLedger
	}
	return st
}

// typeBreakdown counts events per type, most common first.
func typeBreakdown(events []source.Event) []source.TypeCount {
	counts := make(map[string]int64)
	for _, e := range events {
		counts[e.Type]++
	}

	out := make([]source.TypeCount, 0, len(counts))
	for t, n := range counts {
		out = append(out, source.TypeCount{Type: t, Count: n})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].Type < out[j].Type
	})
	return out
}
