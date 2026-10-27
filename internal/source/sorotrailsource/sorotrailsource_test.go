package sorotrailsource

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorolens/internal/source"
)

// fakeIndexer is an in-memory stand-in for a SoroTrail instance. It reproduces
// the behaviour that shapes this package: events come back ascending by ID,
// filtered by ledger range, one page at a time.
type fakeIndexer struct {
	mu sync.Mutex

	events []source.Event
	// pageSize caps each response, mimicking SoroTrail's own limit.
	pageSize int
	calls    int
	failWith error
}

func newFakeIndexer(events []source.Event) *fakeIndexer {
	sorted := make([]source.Event, len(events))
	copy(sorted, events)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })
	return &fakeIndexer{events: sorted, pageSize: maxUpstreamLimit}
}

func (f *fakeIndexer) Events(_ context.Context, q upstreamQuery) (upstreamPage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls++
	if f.failWith != nil {
		return upstreamPage{}, f.failWith
	}

	var matched []source.Event
	for _, e := range f.events {
		if q.ContractID != "" && e.ContractID != q.ContractID {
			continue
		}
		if q.Type != "" && e.Type != q.Type {
			continue
		}
		if q.FromLedger > 0 && e.Ledger < q.FromLedger {
			continue
		}
		if q.ToLedger > 0 && e.Ledger > q.ToLedger {
			continue
		}
		// SoroTrail's cursor is the last ID of the previous page, ascending.
		if q.Cursor != "" && e.ID <= q.Cursor {
			continue
		}
		matched = append(matched, e)
	}

	limit := q.Limit
	if limit <= 0 || limit > f.pageSize {
		limit = f.pageSize
	}

	page := upstreamPage{}
	if len(matched) > limit {
		page.Events = matched[:limit]
		page.Cursor = page.Events[len(page.Events)-1].ID
	} else {
		page.Events = matched
	}
	return page, nil
}

func (f *fakeIndexer) Event(_ context.Context, id string) (source.Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, e := range f.events {
		if e.ID == id {
			return e, nil
		}
	}
	return source.Event{}, source.ErrNotFound
}

func (f *fakeIndexer) Stats(context.Context) (upstreamStats, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.failWith != nil {
		return upstreamStats{}, f.failWith
	}

	contracts := map[string]struct{}{}
	var last int64
	for _, e := range f.events {
		contracts[e.ContractID] = struct{}{}
		if e.Ledger > last {
			last = e.Ledger
		}
	}
	return upstreamStats{
		TotalEvents:        int64(len(f.events)),
		LastIngestedLedger: last,
		ContractCount:      int64(len(contracts)),
	}, nil
}

func (f *fakeIndexer) Health(context.Context) (upstreamHealth, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failWith != nil {
		return upstreamHealth{}, f.failWith
	}
	return upstreamHealth{Status: "ok", Checks: map[string]string{"database": "ok"}}, nil
}

func (f *fakeIndexer) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// makeEvents builds n events spread one per ledger starting at startLedger,
// with IDs that sort lexicographically in chronological order, as TOIDs do.
func makeEvents(n int, startLedger int64, contract string) []source.Event {
	out := make([]source.Event, 0, n)
	for i := 0; i < n; i++ {
		ledger := startLedger + int64(i)
		out = append(out, source.Event{
			ID:               fmt.Sprintf("%019d-%010d", ledger, 1),
			ContractID:       contract,
			Ledger:           ledger,
			Type:             "contract",
			TxHash:           fmt.Sprintf("hash%d", ledger),
			InSuccessfulCall: true,
			Topics:           json.RawMessage(`[{"symbol":"transfer"}]`),
			Value:            json.RawMessage(`{"u32":1}`),
			LedgerClosedAt:   time.Unix(1700000000+ledger, 0).UTC(),
		})
	}
	return out
}

// newTestSource wires a Source onto a fake indexer.
func newTestSource(f *fakeIndexer) *Source {
	return &Source{api: f}
}

func TestListEventsReturnsNewestFirst(t *testing.T) {
	// The upstream API only pages ascending, so this is the core behaviour:
	// SoroLens must still present the newest events first.
	f := newFakeIndexer(makeEvents(100, 1000, "CABC"))
	src := newTestSource(f)

	page, err := src.ListEvents(context.Background(), source.EventQuery{Limit: 10})
	require.NoError(t, err)
	require.Len(t, page.Events, 10)

	assert.Equal(t, int64(1099), page.Events[0].Ledger, "expected the newest event first")
	assert.Equal(t, int64(1090), page.Events[9].Ledger)

	for i := 1; i < len(page.Events); i++ {
		assert.Greater(t, page.Events[i-1].ID, page.Events[i].ID,
			"events must be strictly descending by ID")
	}
	assert.NotEmpty(t, page.NextCursor, "more events remain, so a cursor is expected")
}

func TestListEventsPaginatesWithoutRepeating(t *testing.T) {
	const total = 120
	f := newFakeIndexer(makeEvents(total, 5000, "CABC"))
	src := newTestSource(f)

	seen := map[string]bool{}
	var ordered []string
	cursor := ""

	for page := 0; page < 20; page++ {
		got, err := src.ListEvents(context.Background(), source.EventQuery{Limit: 25, Cursor: cursor})
		require.NoError(t, err)

		for _, e := range got.Events {
			require.False(t, seen[e.ID], "event %s was returned on more than one page", e.ID)
			seen[e.ID] = true
			ordered = append(ordered, e.ID)
		}

		cursor = got.NextCursor
		if cursor == "" {
			break
		}
	}

	assert.Len(t, seen, total, "every event should be reachable by paging")

	// The concatenation of all pages must still be globally descending.
	for i := 1; i < len(ordered); i++ {
		assert.Greater(t, ordered[i-1], ordered[i], "ordering broke across a page boundary")
	}
}

func TestListEventsHandlesDenseLedgers(t *testing.T) {
	// Many events packed into few ledgers is the case that makes a naive
	// backwards scan page through everything. The scanner should narrow its
	// window instead of walking the whole history.
	var events []source.Event
	for ledger := int64(1000); ledger < 1010; ledger++ {
		for i := 0; i < 300; i++ {
			events = append(events, source.Event{
				ID:         fmt.Sprintf("%019d-%010d", ledger, i),
				ContractID: "CABC",
				Ledger:     ledger,
				Type:       "contract",
				Topics:     json.RawMessage(`[{"symbol":"transfer"}]`),
				Value:      json.RawMessage(`{"u32":1}`),
			})
		}
	}

	f := newFakeIndexer(events)
	src := newTestSource(f)

	page, err := src.ListEvents(context.Background(), source.EventQuery{Limit: 20})
	require.NoError(t, err)
	require.Len(t, page.Events, 20)

	// The newest event overall lives in the last ledger.
	assert.Equal(t, int64(1009), page.Events[0].Ledger)
	for i := 1; i < len(page.Events); i++ {
		assert.Greater(t, page.Events[i-1].ID, page.Events[i].ID)
	}
}

func TestListEventsRespectsLedgerBounds(t *testing.T) {
	f := newFakeIndexer(makeEvents(200, 1000, "CABC"))
	src := newTestSource(f)

	page, err := src.ListEvents(context.Background(), source.EventQuery{
		FromLedger: 1050,
		ToLedger:   1060,
		Limit:      50,
	})
	require.NoError(t, err)

	require.NotEmpty(t, page.Events)
	assert.Len(t, page.Events, 11, "ledgers 1050 through 1060 inclusive")
	for _, e := range page.Events {
		assert.GreaterOrEqual(t, e.Ledger, int64(1050))
		assert.LessOrEqual(t, e.Ledger, int64(1060))
	}
	assert.Equal(t, int64(1060), page.Events[0].Ledger)
}

func TestListEventsFiltersByContract(t *testing.T) {
	events := append(makeEvents(20, 1000, "CABC"), makeEvents(20, 1000, "CXYZ")...)
	// Give the second contract distinct IDs so both sets coexist.
	for i := range events[20:] {
		events[20+i].ID = fmt.Sprintf("%019d-%010d", events[20+i].Ledger, 2)
	}

	f := newFakeIndexer(events)
	src := newTestSource(f)

	page, err := src.ListEvents(context.Background(), source.EventQuery{
		ContractID: "CXYZ",
		Limit:      50,
	})
	require.NoError(t, err)

	require.NotEmpty(t, page.Events)
	for _, e := range page.Events {
		assert.Equal(t, "CXYZ", e.ContractID)
	}
}

func TestListEventsOnEmptyIndexer(t *testing.T) {
	src := newTestSource(newFakeIndexer(nil))

	page, err := src.ListEvents(context.Background(), source.EventQuery{Limit: 10})
	require.NoError(t, err)
	assert.Empty(t, page.Events)
	assert.Empty(t, page.NextCursor)
	assert.NotNil(t, page.Events, "an empty page should serialize as [] rather than null")
}

func TestListEventsRejectsBadCursor(t *testing.T) {
	src := newTestSource(newFakeIndexer(makeEvents(10, 1000, "CABC")))

	_, err := src.ListEvents(context.Background(), source.EventQuery{Cursor: "!!!not-base64!!!"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid cursor")
}

func TestGetEvent(t *testing.T) {
	events := makeEvents(5, 1000, "CABC")
	src := newTestSource(newFakeIndexer(events))

	t.Run("found", func(t *testing.T) {
		got, err := src.GetEvent(context.Background(), events[2].ID)
		require.NoError(t, err)
		assert.Equal(t, events[2].ID, got.ID)
		assert.Equal(t, int64(1002), got.Ledger)
	})

	t.Run("missing maps to ErrNotFound", func(t *testing.T) {
		_, err := src.GetEvent(context.Background(), "nope")
		assert.ErrorIs(t, err, source.ErrNotFound)
	})
}

func TestListContractsAggregatesFromEvents(t *testing.T) {
	// SoroTrail exposes no contracts endpoint, so the list is derived. Two
	// contracts with different activity should come back most-recent first.
	var events []source.Event
	events = append(events, makeEvents(5, 1000, "CAAA")...)
	older := makeEvents(3, 100, "CBBB")
	for i := range older {
		older[i].ID = fmt.Sprintf("%019d-%010d", older[i].Ledger, 2)
	}
	events = append(events, older...)

	src := newTestSource(newFakeIndexer(events))

	page, err := src.ListContracts(context.Background(), source.ContractQuery{Limit: 10})
	require.NoError(t, err)
	require.Len(t, page.Contracts, 2)

	assert.Equal(t, "CAAA", page.Contracts[0].ID, "most recently active contract comes first")
	assert.Equal(t, int64(5), page.Contracts[0].EventCount)
	assert.Equal(t, int64(1000), page.Contracts[0].FirstLedger)
	assert.Equal(t, int64(1004), page.Contracts[0].LastLedger)

	assert.Equal(t, "CBBB", page.Contracts[1].ID)
	assert.Equal(t, int64(3), page.Contracts[1].EventCount)
}

func TestListContractsSearchAndPaging(t *testing.T) {
	var events []source.Event
	for i := 0; i < 5; i++ {
		batch := makeEvents(2, int64(1000+i*10), fmt.Sprintf("CONTRACT%d", i))
		for j := range batch {
			batch[j].ID = fmt.Sprintf("%019d-%010d", batch[j].Ledger, i)
		}
		events = append(events, batch...)
	}
	src := newTestSource(newFakeIndexer(events))

	t.Run("search narrows the list", func(t *testing.T) {
		page, err := src.ListContracts(context.Background(), source.ContractQuery{Search: "contract3"})
		require.NoError(t, err)
		require.Len(t, page.Contracts, 1)
		assert.Equal(t, "CONTRACT3", page.Contracts[0].ID)
	})

	t.Run("paging walks the whole list without repeats", func(t *testing.T) {
		seen := map[string]bool{}
		cursor := ""
		for i := 0; i < 10; i++ {
			page, err := src.ListContracts(context.Background(), source.ContractQuery{Limit: 2, Cursor: cursor})
			require.NoError(t, err)
			for _, c := range page.Contracts {
				require.False(t, seen[c.ID], "contract %s repeated across pages", c.ID)
				seen[c.ID] = true
			}
			cursor = page.NextCursor
			if cursor == "" {
				break
			}
		}
		assert.Len(t, seen, 5)
	})
}

func TestContractListIsCached(t *testing.T) {
	// Deriving the list costs several upstream calls, so it must be reused
	// within the TTL rather than recomputed per request.
	f := newFakeIndexer(makeEvents(20, 1000, "CABC"))
	src := newTestSource(f)

	_, err := src.ListContracts(context.Background(), source.ContractQuery{})
	require.NoError(t, err)
	afterFirst := f.callCount()
	require.Positive(t, afterFirst)

	_, err = src.ListContracts(context.Background(), source.ContractQuery{})
	require.NoError(t, err)
	assert.Equal(t, afterFirst, f.callCount(), "the second call should be served from cache")
}

func TestStatsUsesUpstreamTotals(t *testing.T) {
	f := newFakeIndexer(makeEvents(40, 1000, "CABC"))
	src := newTestSource(f)

	stats, err := src.Stats(context.Background())
	require.NoError(t, err)

	// Totals come straight from SoroTrail and are exact.
	assert.Equal(t, int64(40), stats.TotalEvents)
	assert.Equal(t, int64(1), stats.ContractCount)
	assert.Equal(t, int64(1039), stats.LastLedger)
	// The type breakdown is derived from a bounded scan, so the result is
	// flagged approximate rather than implying exactness.
	assert.True(t, stats.Approximate)
	require.NotEmpty(t, stats.TypeBreakdown)
	assert.Equal(t, "contract", stats.TypeBreakdown[0].Type)
}

func TestContractStats(t *testing.T) {
	f := newFakeIndexer(makeEvents(30, 2000, "CABC"))
	src := newTestSource(f)

	stats, err := src.ContractStats(context.Background(), "CABC")
	require.NoError(t, err)

	assert.Equal(t, "CABC", stats.ContractID)
	assert.Equal(t, int64(30), stats.TotalEvents)
	assert.Equal(t, int64(2000), stats.FirstLedger)
	assert.Equal(t, int64(2029), stats.LastLedger)
	require.Len(t, stats.TypeBreakdown, 1)
	assert.Equal(t, int64(30), stats.TypeBreakdown[0].Count)
}

func TestEventBuckets(t *testing.T) {
	base := time.Unix(1700001000, 0).UTC()
	events := []source.Event{
		{ID: "0000001000-0000000001", ContractID: "CABC", Ledger: 1000, Type: "contract", LedgerClosedAt: base},
		{ID: "0000001001-0000000001", ContractID: "CABC", Ledger: 1001, Type: "contract", LedgerClosedAt: base.Add(30 * time.Second)},
		// 1m .. 2m is a gap with 0 events
		{ID: "0000001002-0000000001", ContractID: "CABC", Ledger: 1002, Type: "contract", LedgerClosedAt: base.Add(2 * time.Minute)},
	}
	src := newTestSource(newFakeIndexer(events))
	ctx := context.Background()

	series, err := src.EventBuckets(ctx, source.BucketQuery{
		ContractID: "CABC",
		From:       base,
		To:         base.Add(3 * time.Minute),
		Interval:   time.Minute,
	})
	require.NoError(t, err)
	require.Len(t, series.Buckets, 3)
	assert.Equal(t, int64(2), series.Buckets[0].Count)
	assert.Equal(t, int64(0), series.Buckets[1].Count, "quiet interval must be preserved with zero count")
	assert.Equal(t, int64(1), series.Buckets[2].Count)
	assert.Equal(t, int64(3), series.TotalEvents)
	assert.False(t, series.Approximate, "scan exhausted history so counts are exact")

	cStats, err := src.ContractStats(ctx, "CABC")
	require.NoError(t, err)
	assert.Equal(t, cStats.TotalEvents, series.TotalEvents, "bucket sum over full range must match ContractStats total")
}

func TestStatusReportsHealth(t *testing.T) {
	t.Run("healthy", func(t *testing.T) {
		src := newTestSource(newFakeIndexer(makeEvents(5, 1000, "CABC")))
		st := src.Status(context.Background())

		assert.Equal(t, "sorotrail", st.Mode)
		assert.True(t, st.Healthy)
		assert.Equal(t, int64(1004), st.LatestLedger)
		assert.NotEmpty(t, st.RetentionNote)
	})

	t.Run("unreachable indexer is unhealthy, not an error", func(t *testing.T) {
		f := newFakeIndexer(nil)
		f.failWith = fmt.Errorf("connection refused")
		src := newTestSource(f)

		st := src.Status(context.Background())
		assert.False(t, st.Healthy)
		assert.Contains(t, st.Detail, "connection refused")
	})
}

func TestCursorRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		id     string
		ledger int64
	}{
		{"typical", "0000000000000001000-0000000001", 1000},
		{"empty id", "", 42},
		{"zero ledger", "abc", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, ledger, err := decodeCursor(encodeCursor(tt.id, tt.ledger))
			require.NoError(t, err)
			assert.Equal(t, tt.id, id)
			assert.Equal(t, tt.ledger, ledger)
		})
	}

	t.Run("garbage is rejected", func(t *testing.T) {
		_, _, err := decodeCursor("!!!")
		require.Error(t, err)
	})
}

func TestSortDescDeduplicates(t *testing.T) {
	// Overlapping windows can yield the same event twice; the scan must not
	// surface duplicates to the caller.
	events := []source.Event{
		{ID: "b"}, {ID: "a"}, {ID: "c"}, {ID: "b"},
	}
	got := sortDesc(events)

	require.Len(t, got, 3)
	assert.Equal(t, []string{"c", "b", "a"}, []string{got[0].ID, got[1].ID, got[2].ID})
}

// TestHTTPAPIAgainstRealServer exercises the HTTP layer against a stub of
// SoroTrail's actual response shapes, confirming SoroLens parses what
// SoroTrail really returns.
func TestHTTPAPIAgainstRealServer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/events":
			assert.Equal(t, "CABC", r.URL.Query().Get("contract_id"))
			assert.Equal(t, "100", r.URL.Query().Get("from_ledger"))
			_, _ = w.Write([]byte(`{"events":[{
				"id":"0000000100-0000000001",
				"contract_id":"CABC",
				"ledger":100,
				"type":"contract",
				"tx_hash":"abc",
				"tx_index":1,
				"op_index":2,
				"in_successful_call":true,
				"topics":[{"symbol":"transfer"}],
				"value":{"i128":"1000"},
				"created_at":"2024-01-01T00:00:00Z"
			}],"cursor":"0000000100-0000000001"}`))
		case "/events/0000000100-0000000001":
			_, _ = w.Write([]byte(`{"id":"0000000100-0000000001","contract_id":"CABC","ledger":100,"type":"contract"}`))
		case "/events/missing":
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"not found"}`))
		case "/stats":
			_, _ = w.Write([]byte(`{"total_events":500,"last_ingested_ledger":900,"contract_count":7,"watched_contracts":0}`))
		case "/health":
			_, _ = w.Write([]byte(`{"status":"ok","checks":{"database":"ok","rpc":"ok"}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	api := newHTTPAPI(srv.URL, srv.Client())
	ctx := context.Background()

	t.Run("events", func(t *testing.T) {
		page, err := api.Events(ctx, upstreamQuery{ContractID: "CABC", FromLedger: 100, Limit: 50})
		require.NoError(t, err)

		require.Len(t, page.Events, 1)
		e := page.Events[0]
		assert.Equal(t, "0000000100-0000000001", e.ID)
		assert.Equal(t, "CABC", e.ContractID)
		assert.Equal(t, int64(100), e.Ledger)
		assert.Equal(t, int32(1), e.TxIndex)
		assert.True(t, e.InSuccessfulCall)
		assert.JSONEq(t, `[{"symbol":"transfer"}]`, string(e.Topics))
		assert.Equal(t, "0000000100-0000000001", page.Cursor)
	})

	t.Run("single event", func(t *testing.T) {
		e, err := api.Event(ctx, "0000000100-0000000001")
		require.NoError(t, err)
		assert.Equal(t, "CABC", e.ContractID)
	})

	t.Run("missing event", func(t *testing.T) {
		_, err := api.Event(ctx, "missing")
		assert.ErrorIs(t, err, source.ErrNotFound)
	})

	t.Run("stats", func(t *testing.T) {
		got, err := api.Stats(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(500), got.TotalEvents)
		assert.Equal(t, int64(900), got.LastIngestedLedger)
		assert.Equal(t, int64(7), got.ContractCount)
	})

	t.Run("health", func(t *testing.T) {
		got, err := api.Health(ctx)
		require.NoError(t, err)
		assert.Equal(t, "ok", got.Status)
	})
}

func TestNewTrimsTrailingSlash(t *testing.T) {
	// A trailing slash would otherwise produce //events in every request.
	src := New("http://example.com/", nil)
	api, ok := src.api.(*httpAPI)
	require.True(t, ok)
	assert.Equal(t, "http://example.com", api.baseURL)
}
