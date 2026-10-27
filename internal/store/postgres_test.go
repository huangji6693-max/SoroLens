package store

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorolens/internal/source"
)

// newTestStore connects to the database named by TEST_DATABASE_URL, applies
// the migrations and truncates the tables. Without that variable the whole
// store suite skips, so `go test ./...` stays green with no Postgres running.
//
// Run these with `make test-db`, which points at the docker-compose database.
func newTestStore(t *testing.T) *Postgres {
	t.Helper()

	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set; skipping the Postgres integration tests (see make test-db)")
	}

	require.NoError(t, Migrate(databaseURL))

	ctx := context.Background()
	pg, err := NewPostgres(ctx, databaseURL)
	require.NoError(t, err)
	t.Cleanup(pg.Close)

	_, err = pg.pool.Exec(ctx, `TRUNCATE events`)
	require.NoError(t, err)
	_, err = pg.pool.Exec(ctx, `UPDATE ingest_state SET last_ledger = 0, last_cursor = '' WHERE id = 1`)
	require.NoError(t, err)

	return pg
}

// event builds a test event whose ID sorts chronologically, as TOIDs do.
func event(ledger int64, seq int, contract string) source.Event {
	return source.Event{
		ID:               fmt.Sprintf("%019d-%010d", ledger, seq),
		ContractID:       contract,
		Ledger:           ledger,
		Type:             "contract",
		TxHash:           fmt.Sprintf("hash-%d-%d", ledger, seq),
		TxIndex:          int32(seq),
		OpIndex:          0,
		InSuccessfulCall: true,
		Topics:           json.RawMessage(`[{"symbol":"transfer"},{"address":"GABC"}]`),
		Value:            json.RawMessage(`{"i128":"1000"}`),
		LedgerClosedAt:   time.Unix(1700000000+ledger, 0).UTC(),
	}
}

func TestUpsertEventsIsIdempotent(t *testing.T) {
	pg := newTestStore(t)
	ctx := context.Background()

	events := []source.Event{event(100, 1, "CAAA"), event(101, 1, "CAAA")}

	inserted, err := pg.UpsertEvents(ctx, events)
	require.NoError(t, err)
	assert.Equal(t, int64(2), inserted)

	// Re-ingesting the same ledger range must not duplicate rows.
	inserted, err = pg.UpsertEvents(ctx, events)
	require.NoError(t, err)
	assert.Zero(t, inserted, "re-reading a ledger range should insert nothing")

	stats, err := pg.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(2), stats.TotalEvents)
}

func TestUpsertEventsEmptySlice(t *testing.T) {
	pg := newTestStore(t)

	inserted, err := pg.UpsertEvents(context.Background(), nil)
	require.NoError(t, err)
	assert.Zero(t, inserted)
}

func TestUpsertEventsDefaultsMissingJSON(t *testing.T) {
	pg := newTestStore(t)
	ctx := context.Background()

	// An event with no topics or value must still store cleanly, since the
	// columns are NOT NULL.
	e := event(100, 1, "CAAA")
	e.Topics, e.Value = nil, nil

	_, err := pg.UpsertEvents(ctx, []source.Event{e})
	require.NoError(t, err)

	got, err := pg.GetEvent(ctx, e.ID)
	require.NoError(t, err)
	assert.JSONEq(t, `[]`, string(got.Topics))
	assert.Equal(t, "null", string(got.Value))
}

func TestGetEvent(t *testing.T) {
	pg := newTestStore(t)
	ctx := context.Background()

	e := event(100, 1, "CAAA")
	_, err := pg.UpsertEvents(ctx, []source.Event{e})
	require.NoError(t, err)

	t.Run("found", func(t *testing.T) {
		got, err := pg.GetEvent(ctx, e.ID)
		require.NoError(t, err)

		assert.Equal(t, e.ID, got.ID)
		assert.Equal(t, "CAAA", got.ContractID)
		assert.Equal(t, int64(100), got.Ledger)
		assert.Equal(t, e.TxHash, got.TxHash)
		assert.True(t, got.InSuccessfulCall)
		assert.JSONEq(t, string(e.Topics), string(got.Topics))
		assert.False(t, got.CreatedAt.IsZero())
	})

	t.Run("missing", func(t *testing.T) {
		_, err := pg.GetEvent(ctx, "does-not-exist")
		assert.ErrorIs(t, err, source.ErrNotFound)
	})
}

func TestQueryEventsOrdersNewestFirst(t *testing.T) {
	pg := newTestStore(t)
	ctx := context.Background()

	var events []source.Event
	for ledger := int64(100); ledger < 110; ledger++ {
		events = append(events, event(ledger, 1, "CAAA"))
	}
	_, err := pg.UpsertEvents(ctx, events)
	require.NoError(t, err)

	got, _, err := pg.QueryEvents(ctx, source.EventQuery{})
	require.NoError(t, err)
	require.Len(t, got, 10)

	assert.Equal(t, int64(109), got[0].Ledger, "newest event first")
	for i := 1; i < len(got); i++ {
		assert.Greater(t, got[i-1].ID, got[i].ID)
	}
}

func TestQueryEventsPagination(t *testing.T) {
	pg := newTestStore(t)
	ctx := context.Background()

	var events []source.Event
	for ledger := int64(100); ledger < 125; ledger++ {
		events = append(events, event(ledger, 1, "CAAA"))
	}
	_, err := pg.UpsertEvents(ctx, events)
	require.NoError(t, err)

	seen := map[string]bool{}
	cursor := ""
	pages := 0

	for {
		page, next, err := pg.QueryEvents(ctx, source.EventQuery{Limit: 10, Cursor: cursor})
		require.NoError(t, err)
		pages++

		for _, e := range page {
			require.False(t, seen[e.ID], "event %s appeared on two pages", e.ID)
			seen[e.ID] = true
		}

		cursor = next
		if cursor == "" {
			break
		}
		require.Less(t, pages, 10, "pagination did not terminate")
	}

	assert.Len(t, seen, 25, "every event should be reachable")
	assert.Equal(t, 3, pages)
}

func TestQueryEventsFilters(t *testing.T) {
	pg := newTestStore(t)
	ctx := context.Background()

	diagnostic := event(105, 2, "CAAA")
	diagnostic.Type = "diagnostic"
	diagnostic.Topics = json.RawMessage(`[{"symbol":"mint"}]`)

	events := []source.Event{
		event(100, 1, "CAAA"),
		event(105, 1, "CAAA"),
		event(110, 1, "CBBB"),
		diagnostic,
	}
	_, err := pg.UpsertEvents(ctx, events)
	require.NoError(t, err)

	t.Run("by contract", func(t *testing.T) {
		got, _, err := pg.QueryEvents(ctx, source.EventQuery{ContractID: "CBBB"})
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "CBBB", got[0].ContractID)
	})

	t.Run("by type", func(t *testing.T) {
		got, _, err := pg.QueryEvents(ctx, source.EventQuery{Type: "diagnostic"})
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, "diagnostic", got[0].Type)
	})

	t.Run("by ledger range", func(t *testing.T) {
		got, _, err := pg.QueryEvents(ctx, source.EventQuery{FromLedger: 105, ToLedger: 105})
		require.NoError(t, err)
		assert.Len(t, got, 2, "both events in ledger 105")
		for _, e := range got {
			assert.Equal(t, int64(105), e.Ledger)
		}
	})

	t.Run("by topic containment", func(t *testing.T) {
		got, _, err := pg.QueryEvents(ctx, source.EventQuery{
			Topic: json.RawMessage(`{"symbol":"mint"}`),
		})
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, diagnostic.ID, got[0].ID)
	})

	t.Run("topic matches at any position", func(t *testing.T) {
		// The address is the second topic, so this proves containment is not
		// anchored to the first element.
		got, _, err := pg.QueryEvents(ctx, source.EventQuery{
			Topic: json.RawMessage(`{"address":"GABC"}`),
		})
		require.NoError(t, err)
		assert.Len(t, got, 3, "the three events carrying that address topic")
	})

	t.Run("combined filters", func(t *testing.T) {
		got, _, err := pg.QueryEvents(ctx, source.EventQuery{
			ContractID: "CAAA",
			Type:       "contract",
			FromLedger: 105,
		})
		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, int64(105), got[0].Ledger)
	})
}

func TestQueryEventsRejectsBadCursorGracefully(t *testing.T) {
	pg := newTestStore(t)

	// An unparseable cursor is compared as a plain string, which simply
	// matches nothing rather than erroring.
	got, next, err := pg.QueryEvents(context.Background(), source.EventQuery{Cursor: "!!!"})
	require.NoError(t, err)
	assert.Empty(t, got)
	assert.Empty(t, next)
}

func TestListContracts(t *testing.T) {
	pg := newTestStore(t)
	ctx := context.Background()

	events := []source.Event{
		event(100, 1, "CAAA"),
		event(105, 1, "CAAA"),
		event(110, 1, "CAAA"),
		event(200, 1, "CBBB"),
	}
	_, err := pg.UpsertEvents(ctx, events)
	require.NoError(t, err)

	got, _, err := pg.ListContracts(ctx, source.ContractQuery{})
	require.NoError(t, err)
	require.Len(t, got, 2)

	// Most recently active first.
	assert.Equal(t, "CBBB", got[0].ID)
	assert.Equal(t, int64(1), got[0].EventCount)
	assert.Equal(t, int64(200), got[0].LastLedger)

	assert.Equal(t, "CAAA", got[1].ID)
	assert.Equal(t, int64(3), got[1].EventCount)
	assert.Equal(t, int64(100), got[1].FirstLedger)
	assert.Equal(t, int64(110), got[1].LastLedger)
	assert.False(t, got[1].LastActivity.IsZero())
}

func TestListContractsSearch(t *testing.T) {
	pg := newTestStore(t)
	ctx := context.Background()

	_, err := pg.UpsertEvents(ctx, []source.Event{
		event(100, 1, "CAAA"),
		event(200, 1, "CBBB"),
	})
	require.NoError(t, err)

	got, _, err := pg.ListContracts(ctx, source.ContractQuery{Search: "BBB"})
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "CBBB", got[0].ID)

	t.Run("search is case-insensitive", func(t *testing.T) {
		got, _, err := pg.ListContracts(ctx, source.ContractQuery{Search: "bbb"})
		require.NoError(t, err)
		require.Len(t, got, 1)
	})
}

func TestListContractsPagination(t *testing.T) {
	pg := newTestStore(t)
	ctx := context.Background()

	// Several contracts sharing a ledger exercise the composite cursor, which
	// must break ties on contract ID rather than looping.
	var events []source.Event
	for i := 0; i < 10; i++ {
		events = append(events, event(100, i, fmt.Sprintf("C%03d", i)))
	}
	_, err := pg.UpsertEvents(ctx, events)
	require.NoError(t, err)

	seen := map[string]bool{}
	cursor := ""
	for page := 0; page < 10; page++ {
		got, next, err := pg.ListContracts(ctx, source.ContractQuery{Limit: 3, Cursor: cursor})
		require.NoError(t, err)

		for _, c := range got {
			require.False(t, seen[c.ID], "contract %s repeated across pages", c.ID)
			seen[c.ID] = true
		}

		cursor = next
		if cursor == "" {
			break
		}
	}

	assert.Len(t, seen, 10, "every contract should be reachable by paging")
}

func TestListContractsRejectsBadCursor(t *testing.T) {
	pg := newTestStore(t)

	_, _, err := pg.ListContracts(context.Background(), source.ContractQuery{Cursor: "!!!not-base64"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid cursor")
}

func TestContractStats(t *testing.T) {
	pg := newTestStore(t)
	ctx := context.Background()

	diagnostic := event(120, 2, "CAAA")
	diagnostic.Type = "diagnostic"

	_, err := pg.UpsertEvents(ctx, []source.Event{
		event(100, 1, "CAAA"),
		event(110, 1, "CAAA"),
		diagnostic,
		event(200, 1, "CBBB"),
	})
	require.NoError(t, err)

	got, err := pg.ContractStats(ctx, "CAAA")
	require.NoError(t, err)

	assert.Equal(t, "CAAA", got.ContractID)
	assert.Equal(t, int64(3), got.TotalEvents)
	assert.Equal(t, int64(100), got.FirstLedger)
	assert.Equal(t, int64(120), got.LastLedger)
	assert.False(t, got.Approximate, "standalone counts are exact")

	require.Len(t, got.TypeBreakdown, 2)
	assert.Equal(t, "contract", got.TypeBreakdown[0].Type, "most common type first")
	assert.Equal(t, int64(2), got.TypeBreakdown[0].Count)

	t.Run("unknown contract is empty, not an error", func(t *testing.T) {
		got, err := pg.ContractStats(ctx, "CZZZ")
		require.NoError(t, err)
		assert.Zero(t, got.TotalEvents)
	})
}

func TestStats(t *testing.T) {
	pg := newTestStore(t)
	ctx := context.Background()

	t.Run("empty store", func(t *testing.T) {
		got, err := pg.Stats(ctx)
		require.NoError(t, err)
		assert.Zero(t, got.TotalEvents)
		assert.Zero(t, got.ContractCount)
		assert.Zero(t, got.FirstLedger)
	})

	_, err := pg.UpsertEvents(ctx, []source.Event{
		event(100, 1, "CAAA"),
		event(110, 1, "CAAA"),
		event(200, 1, "CBBB"),
	})
	require.NoError(t, err)

	got, err := pg.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(3), got.TotalEvents)
	assert.Equal(t, int64(2), got.ContractCount)
	assert.Equal(t, int64(100), got.FirstLedger)
	assert.Equal(t, int64(200), got.LastLedger)
	assert.False(t, got.Approximate)
	require.Len(t, got.TypeBreakdown, 1)
}

func TestEventBuckets(t *testing.T) {
	pg := newTestStore(t)
	ctx := context.Background()

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	mk := func(seq int, contract, typ string, at time.Time) source.Event {
		e := event(int64(100+seq), seq, contract)
		e.Type = typ
		e.LedgerClosedAt = at
		return e
	}

	_, err := pg.UpsertEvents(ctx, []source.Event{
		mk(1, "CAAA", "contract", base),                                     // bucket 0 start boundary (included)
		mk(2, "CAAA", "contract", base.Add(30*time.Second)),                 // bucket 0 interior
		mk(3, "CAAA", "contract", base.Add(time.Minute)),                    // bucket 1 start boundary (excluded from 0, included in 1)
		mk(4, "CAAA", "diagnostic", base.Add(3*time.Minute+10*time.Second)), // bucket 3 (bucket 2 is a 0-count gap)
		mk(5, "CBBB", "contract", base.Add(4*time.Minute)),                  // Exact upper bound 'to' for 4m range (excluded)
	})
	require.NoError(t, err)

	t.Run("boundary alignment and zero-count gaps in SQL", func(t *testing.T) {
		series, err := pg.EventBuckets(ctx, source.BucketQuery{
			From:     base,
			To:       base.Add(4 * time.Minute),
			Interval: time.Minute,
		})
		require.NoError(t, err)
		require.Len(t, series.Buckets, 4)
		assert.Equal(t, int64(2), series.Buckets[0].Count)
		assert.Equal(t, int64(1), series.Buckets[1].Count)
		assert.Equal(t, int64(0), series.Buckets[2].Count, "empty bucket must be present with count 0")
		assert.Equal(t, int64(1), series.Buckets[3].Count)
		assert.Equal(t, int64(4), series.TotalEvents, "event at exact upper bound 'to' must be excluded")
	})

	t.Run("empty range returns empty buckets", func(t *testing.T) {
		series, err := pg.EventBuckets(ctx, source.BucketQuery{
			From:     base,
			To:       base,
			Interval: time.Minute,
		})
		require.NoError(t, err)
		assert.Empty(t, series.Buckets)
		assert.Zero(t, series.TotalEvents)
	})

	t.Run("bucket totals agree with Stats and ContractStats over full range", func(t *testing.T) {
		allSeries, err := pg.EventBuckets(ctx, source.BucketQuery{
			From:     base,
			To:       base.Add(5 * time.Minute),
			Interval: time.Minute,
		})
		require.NoError(t, err)
		stats, err := pg.Stats(ctx)
		require.NoError(t, err)
		assert.Equal(t, stats.TotalEvents, allSeries.TotalEvents)

		contractSeries, err := pg.EventBuckets(ctx, source.BucketQuery{
			ContractID: "CAAA",
			From:       base,
			To:         base.Add(5 * time.Minute),
			Interval:   time.Minute,
		})
		require.NoError(t, err)
		cStats, err := pg.ContractStats(ctx, "CAAA")
		require.NoError(t, err)
		assert.Equal(t, cStats.TotalEvents, contractSeries.TotalEvents)
	})
}

func TestIngestState(t *testing.T) {
	pg := newTestStore(t)
	ctx := context.Background()

	t.Run("starts empty", func(t *testing.T) {
		got, err := pg.GetIngestState(ctx)
		require.NoError(t, err)
		assert.Zero(t, got.LastLedger)
	})

	require.NoError(t, pg.SaveIngestState(ctx, IngestState{LastLedger: 500, LastCursor: "abc"}))

	got, err := pg.GetIngestState(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(500), got.LastLedger)
	assert.Equal(t, "abc", got.LastCursor)
	assert.False(t, got.UpdatedAt.IsZero())

	t.Run("saving again overwrites the single row", func(t *testing.T) {
		require.NoError(t, pg.SaveIngestState(ctx, IngestState{LastLedger: 600}))

		got, err := pg.GetIngestState(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(600), got.LastLedger)

		var rows int
		require.NoError(t, pg.pool.QueryRow(ctx, `SELECT count(*) FROM ingest_state`).Scan(&rows))
		assert.Equal(t, 1, rows, "ingest_state must stay a single row")
	})
}

func TestPing(t *testing.T) {
	pg := newTestStore(t)
	assert.NoError(t, pg.Ping(context.Background()))
}

func TestContractCursorRoundTrip(t *testing.T) {
	// This is pure encoding, so it runs without a database.
	tests := []struct {
		name     string
		ledger   int64
		contract string
	}{
		{"typical", 12345, "CAAA"},
		{"zero ledger", 0, "CBBB"},
		{"contract containing a pipe is still recoverable", 5, "C|WEIRD"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ledger, contract, err := decodeContractCursor(encodeContractCursor(tt.ledger, tt.contract))
			require.NoError(t, err)
			assert.Equal(t, tt.ledger, ledger)
			assert.Equal(t, tt.contract, contract)
		})
	}

	t.Run("garbage is rejected", func(t *testing.T) {
		_, _, err := decodeContractCursor("!!!")
		require.Error(t, err)
	})
}
