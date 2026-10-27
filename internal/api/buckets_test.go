package api

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorolens/internal/source"
)

func TestEventBucketsHandlerForwardsFilters(t *testing.T) {
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	src := &fakeSource{
		buckets: source.BucketSeries{
			Interval:    "15m",
			From:        base,
			To:          base.Add(time.Hour),
			TotalEvents: 5,
			Buckets: []source.EventBucket{
				{Start: base, End: base.Add(15 * time.Minute), Count: 2},
				{Start: base.Add(15 * time.Minute), End: base.Add(30 * time.Minute), Count: 0},
				{Start: base.Add(30 * time.Minute), End: base.Add(45 * time.Minute), Count: 3},
				{Start: base.Add(45 * time.Minute), End: base.Add(60 * time.Minute), Count: 0},
			},
		},
	}

	h := testServer(src)

	t.Run("global endpoint parses contract_id, type, topic, from, to, interval", func(t *testing.T) {
		target := "/api/events/buckets?contract_id=" + testContract +
			"&type=contract&topic=transfer&from=2024-01-01T00:00:00Z&to=2024-01-01T01:00:00Z&interval=15m"
		rec := do(t, h, target)
		require.Equal(t, http.StatusOK, rec.Code)

		q := src.lastBucketQuery
		assert.Equal(t, testContract, q.ContractID)
		assert.Equal(t, "contract", q.Type)
		assert.JSONEq(t, `{"symbol":"transfer"}`, string(q.Topic))
		assert.Equal(t, base, q.From)
		assert.Equal(t, base.Add(time.Hour), q.To)
		assert.Equal(t, 15*time.Minute, q.Interval)

		var got source.BucketSeries
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
		assert.Equal(t, "15m", got.Interval)
		assert.Equal(t, int64(5), got.TotalEvents)
		require.Len(t, got.Buckets, 4)
		assert.Zero(t, got.Buckets[1].Count, "empty gap bucket must be present with count 0")
	})

	t.Run("contract sub-resource sets contract ID from path and defaults interval to 1h", func(t *testing.T) {
		target := "/api/contracts/" + testContract + "/buckets?from=2024-01-01T00:00:00Z&to=2024-01-01T06:00:00Z"
		rec := do(t, h, target)
		require.Equal(t, http.StatusOK, rec.Code)

		q := src.lastBucketQuery
		assert.Equal(t, testContract, q.ContractID)
		assert.Equal(t, time.Hour, q.Interval)
	})

	t.Run("day suffix interval 1d is accepted", func(t *testing.T) {
		target := "/api/events/buckets?from=2024-01-01T00:00:00Z&to=2024-01-08T00:00:00Z&interval=1d"
		rec := do(t, h, target)
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, 24*time.Hour, src.lastBucketQuery.Interval)
	})
}

func TestEventBucketsValidationAndCapEnforcement(t *testing.T) {
	h := testServer(&fakeSource{})

	tests := []struct {
		name   string
		target string
		status int
	}{
		{
			name:   "missing from parameter",
			target: "/api/events/buckets?to=2024-01-01T01:00:00Z",
			status: http.StatusBadRequest,
		},
		{
			name:   "missing to parameter",
			target: "/api/events/buckets?from=2024-01-01T00:00:00Z",
			status: http.StatusBadRequest,
		},
		{
			name:   "invalid RFC3339 timestamp",
			target: "/api/events/buckets?from=not-a-timestamp&to=2024-01-01T01:00:00Z",
			status: http.StatusBadRequest,
		},
		{
			name:   "from after to",
			target: "/api/events/buckets?from=2024-01-01T02:00:00Z&to=2024-01-01T01:00:00Z",
			status: http.StatusBadRequest,
		},
		{
			name:   "interval below minimum 1m",
			target: "/api/events/buckets?from=2024-01-01T00:00:00Z&to=2024-01-01T00:10:00Z&interval=10s",
			status: http.StatusBadRequest,
		},
		{
			name:   "interval above maximum 7d",
			target: "/api/events/buckets?from=2024-01-01T00:00:00Z&to=2024-02-01T00:00:00Z&interval=10d",
			status: http.StatusBadRequest,
		},
		{
			name:   "exceeds 500 bucket cap (501 minutes at 1m interval)",
			target: "/api/events/buckets?from=2024-01-01T00:00:00Z&to=2024-01-01T08:21:00Z&interval=1m",
			status: http.StatusBadRequest,
		},
		{
			name:   "exact 500 bucket cap (500 minutes at 1m interval) is allowed",
			target: "/api/events/buckets?from=2024-01-01T00:00:00Z&to=2024-01-01T08:20:00Z&interval=1m",
			status: http.StatusOK,
		},
		{
			name:   "invalid contract_id query parameter",
			target: "/api/events/buckets?contract_id=bad&from=2024-01-01T00:00:00Z&to=2024-01-01T01:00:00Z",
			status: http.StatusBadRequest,
		},
		{
			name:   "invalid contract id path parameter",
			target: "/api/contracts/bad/buckets?from=2024-01-01T00:00:00Z&to=2024-01-01T01:00:00Z",
			status: http.StatusBadRequest,
		},
		{
			name:   "invalid event type",
			target: "/api/events/buckets?type=bogus&from=2024-01-01T00:00:00Z&to=2024-01-01T01:00:00Z",
			status: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := do(t, h, tt.target)
			assert.Equal(t, tt.status, rec.Code)
		})
	}
}

func TestCountEventsIntoBucketsBoundaryAlignmentGapsAndStatsAgreement(t *testing.T) {
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	interval := 15 * time.Minute
	to := base.Add(time.Hour) // 4 buckets: [00:00,00:15), [00:15,00:30), [00:30,00:45), [00:45,01:00)

	t.Run("empty range returns empty slice and zero total", func(t *testing.T) {
		buckets, total := source.CountEventsIntoBuckets(base, base, interval, []source.Event{
			{ID: "1", LedgerClosedAt: base},
		})
		assert.Empty(t, buckets)
		assert.Zero(t, total)
	})

	t.Run("range with no matching events returns all zero-count buckets", func(t *testing.T) {
		buckets, total := source.CountEventsIntoBuckets(base, to, interval, nil)
		require.Len(t, buckets, 4)
		assert.Zero(t, total)
		for i, b := range buckets {
			assert.Equal(t, base.Add(time.Duration(i)*interval), b.Start)
			assert.Equal(t, base.Add(time.Duration(i+1)*interval), b.End)
			assert.Zero(t, b.Count)
		}
	})

	t.Run("half-open boundary alignment, gaps, and agreement with total stats", func(t *testing.T) {
		inRangeEvents := []source.Event{
			// Exact lower bound of range -> lands in bucket 0 [00:00, 00:15)
			{ID: "e1", LedgerClosedAt: base},
			// Inside bucket 0
			{ID: "e2", LedgerClosedAt: base.Add(14*time.Minute + 59*time.Second)},
			// Exact boundary between bucket 0 and bucket 1 -> lands in bucket 1 [00:15, 00:30)
			{ID: "e3", LedgerClosedAt: base.Add(15 * time.Minute)},
			// Bucket 2 [00:30, 00:45) intentionally left empty (gap)
			// Inside bucket 3 [00:45, 01:00)
			{ID: "e4", LedgerClosedAt: base.Add(45 * time.Minute)},
			{ID: "e5", LedgerClosedAt: base.Add(59 * time.Minute)},
		}
		outOfRangeEvents := []source.Event{
			// 1ns before from -> excluded
			{ID: "e0", LedgerClosedAt: base.Add(-time.Nanosecond)},
			// Exact upper bound to -> excluded by half-open [from, to)
			{ID: "e6", LedgerClosedAt: to},
		}
		all := append(append([]source.Event{}, inRangeEvents...), outOfRangeEvents...)

		buckets, total := source.CountEventsIntoBuckets(base, to, interval, all)
		require.Len(t, buckets, 4)
		assert.Equal(t, int64(2), buckets[0].Count, "bucket 0 [00:00, 00:15)")
		assert.Equal(t, int64(1), buckets[1].Count, "bucket 1 [00:15, 00:30) receives exact boundary event")
		assert.Equal(t, int64(0), buckets[2].Count, "bucket 2 [00:30, 00:45) is a zero-count gap")
		assert.Equal(t, int64(2), buckets[3].Count, "bucket 3 [00:45, 01:00) excludes event at exact 'to'")

		var sum int64
		for _, b := range buckets {
			sum += b.Count
		}
		assert.Equal(t, total, sum)
		assert.Equal(t, int64(len(inRangeEvents)), total,
			"sum of bucket counts must agree with total events in [from, to)")
	})
}
