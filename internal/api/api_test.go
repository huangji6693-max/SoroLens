package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorolens/internal/source"
)

const testContract = "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

// fakeSource is a scripted source.EventSource. It records the query it was
// given so tests can assert on how request parameters were parsed.
type fakeSource struct {
	contracts     source.ContractPage
	events        source.EventPage
	event         source.Event
	stats         source.Stats
	contractStats source.ContractStats
	buckets       source.BucketSeries
	status        source.Status

	err error

	lastEventQuery    source.EventQuery
	lastContractQuery source.ContractQuery
	lastBucketQuery   source.BucketQuery
	lastEventID       string
}

func (f *fakeSource) ListContracts(_ context.Context, q source.ContractQuery) (source.ContractPage, error) {
	f.lastContractQuery = q
	return f.contracts, f.err
}

func (f *fakeSource) ListEvents(_ context.Context, q source.EventQuery) (source.EventPage, error) {
	f.lastEventQuery = q
	return f.events, f.err
}

func (f *fakeSource) GetEvent(_ context.Context, id string) (source.Event, error) {
	f.lastEventID = id
	return f.event, f.err
}

func (f *fakeSource) ContractStats(context.Context, string) (source.ContractStats, error) {
	return f.contractStats, f.err
}

func (f *fakeSource) EventBuckets(_ context.Context, q source.BucketQuery) (source.BucketSeries, error) {
	f.lastBucketQuery = q
	return f.buckets, f.err
}

func (f *fakeSource) Stats(context.Context) (source.Stats, error) { return f.stats, f.err }
func (f *fakeSource) Status(context.Context) source.Status        { return f.status }

func testServer(src source.EventSource) http.Handler {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := New(src, log)

	r := chi.NewRouter()
	r.Get("/health", s.HealthHandler())
	r.Mount("/api", s.Routes())
	return r
}

// do issues a request and returns the recorder.
func do(t *testing.T, h http.Handler, target string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
	return rec
}

func TestListContracts(t *testing.T) {
	src := &fakeSource{contracts: source.ContractPage{
		Contracts: []source.Contract{{
			ID:           testContract,
			EventCount:   12,
			FirstLedger:  100,
			LastLedger:   200,
			LastActivity: time.Unix(1700000000, 0).UTC(),
		}},
		NextCursor: "next-page",
	}}

	rec := do(t, testServer(src), "/api/contracts?search=CDLZ&limit=10")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))

	var got source.ContractPage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Len(t, got.Contracts, 1)
	assert.Equal(t, testContract, got.Contracts[0].ID)
	assert.Equal(t, int64(12), got.Contracts[0].EventCount)
	assert.Equal(t, "next-page", got.NextCursor)

	// Query parameters must reach the source unchanged.
	assert.Equal(t, "CDLZ", src.lastContractQuery.Search)
	assert.Equal(t, 10, src.lastContractQuery.Limit)
}

func TestListEventsParsesFilters(t *testing.T) {
	src := &fakeSource{events: source.EventPage{Events: []source.Event{}}}
	h := testServer(src)

	rec := do(t, h, "/api/events?type=contract&topic=transfer&from_ledger=100&to_ledger=200&limit=25&cursor=abc")
	require.Equal(t, http.StatusOK, rec.Code)

	q := src.lastEventQuery
	assert.Equal(t, "contract", q.Type)
	assert.Equal(t, int64(100), q.FromLedger)
	assert.Equal(t, int64(200), q.ToLedger)
	assert.Equal(t, 25, q.Limit)
	assert.Equal(t, "abc", q.Cursor)
	// A bare word is interpreted as an event name, which is a decoded symbol.
	assert.JSONEq(t, `{"symbol":"transfer"}`, string(q.Topic))
}

func TestListEventsAcceptsJSONTopic(t *testing.T) {
	src := &fakeSource{events: source.EventPage{}}
	rec := do(t, testServer(src), `/api/events?topic=%7B%22address%22%3A%22GABC%22%7D`)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.JSONEq(t, `{"address":"GABC"}`, string(src.lastEventQuery.Topic))
}

func TestListEventsRejectsBadInput(t *testing.T) {
	tests := []struct {
		name   string
		target string
		errMsg string
	}{
		{"unknown type", "/api/events?type=bogus", "invalid type"},
		{"negative ledger", "/api/events?from_ledger=-1", "from_ledger must be a positive integer"},
		{"non-numeric ledger", "/api/events?to_ledger=abc", "to_ledger must be a positive integer"},
		{"inverted range", "/api/events?from_ledger=200&to_ledger=100", "is after to_ledger"},
		{"limit too large", "/api/events?limit=100000", "limit must be an integer"},
		{"limit zero", "/api/events?limit=0", "limit must be an integer"},
	}

	h := testServer(&fakeSource{})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := do(t, h, tt.target)
			require.Equal(t, http.StatusBadRequest, rec.Code)

			var body errorResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			assert.Contains(t, body.Error, tt.errMsg)
		})
	}
}

func TestContractEventsValidatesContractID(t *testing.T) {
	h := testServer(&fakeSource{events: source.EventPage{}})

	t.Run("valid ID reaches the source", func(t *testing.T) {
		src := &fakeSource{events: source.EventPage{}}
		rec := do(t, testServer(src), "/api/contracts/"+testContract+"/events")
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, testContract, src.lastEventQuery.ContractID)
	})

	t.Run("malformed ID is rejected", func(t *testing.T) {
		rec := do(t, h, "/api/contracts/not-a-contract/events")
		require.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "invalid contract ID")
	})
}

func TestGetEvent(t *testing.T) {
	t.Run("found", func(t *testing.T) {
		src := &fakeSource{event: source.Event{
			ID:               "0000000100-0000000001",
			ContractID:       testContract,
			Ledger:           100,
			Type:             "contract",
			TxHash:           "abc",
			InSuccessfulCall: true,
			Topics:           json.RawMessage(`[{"symbol":"transfer"}]`),
			Value:            json.RawMessage(`{"i128":"1000"}`),
		}}

		rec := do(t, testServer(src), "/api/events/0000000100-0000000001")
		require.Equal(t, http.StatusOK, rec.Code)

		var got source.Event
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
		assert.Equal(t, "0000000100-0000000001", got.ID)
		assert.JSONEq(t, `{"i128":"1000"}`, string(got.Value))
		assert.Equal(t, "0000000100-0000000001", src.lastEventID)
	})

	t.Run("missing returns 404", func(t *testing.T) {
		src := &fakeSource{err: source.ErrNotFound}
		rec := do(t, testServer(src), "/api/events/nope")

		require.Equal(t, http.StatusNotFound, rec.Code)
		assert.Contains(t, rec.Body.String(), "not found")
	})
}

func TestStats(t *testing.T) {
	src := &fakeSource{stats: source.Stats{
		TotalEvents:   1000,
		ContractCount: 5,
		FirstLedger:   100,
		LastLedger:    900,
		TypeBreakdown: []source.TypeCount{{Type: "contract", Count: 900}},
		Approximate:   true,
	}}

	rec := do(t, testServer(src), "/api/stats")
	require.Equal(t, http.StatusOK, rec.Code)

	var got source.Stats
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	assert.Equal(t, int64(1000), got.TotalEvents)
	assert.True(t, got.Approximate, "approximation must be visible to API clients")
	require.Len(t, got.TypeBreakdown, 1)
}

func TestContractStats(t *testing.T) {
	src := &fakeSource{contractStats: source.ContractStats{
		ContractID:  testContract,
		TotalEvents: 42,
	}}

	rec := do(t, testServer(src), "/api/contracts/"+testContract+"/stats")
	require.Equal(t, http.StatusOK, rec.Code)

	var got source.ContractStats
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	assert.Equal(t, int64(42), got.TotalEvents)
}

func TestHealth(t *testing.T) {
	t.Run("healthy is 200", func(t *testing.T) {
		src := &fakeSource{status: source.Status{Mode: "rpc", Healthy: true, LatestLedger: 500}}
		rec := do(t, testServer(src), "/health")

		require.Equal(t, http.StatusOK, rec.Code)

		var got source.Status
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
		assert.True(t, got.Healthy)
		assert.Equal(t, "rpc", got.Mode)
	})

	t.Run("unhealthy is 503 so probes fail", func(t *testing.T) {
		src := &fakeSource{status: source.Status{Mode: "rpc", Healthy: false, Detail: "database unreachable"}}
		rec := do(t, testServer(src), "/health")

		require.Equal(t, http.StatusServiceUnavailable, rec.Code)
		assert.Contains(t, rec.Body.String(), "database unreachable")
	})
}

func TestInternalErrorsAreNotLeaked(t *testing.T) {
	// A backend failure must not expose its message, which can carry
	// connection strings or internal hostnames.
	src := &fakeSource{err: errors.New("postgres://user:hunter2@db:5432 connection refused")}

	rec := do(t, testServer(src), "/api/events")
	require.Equal(t, http.StatusInternalServerError, rec.Code)

	body := rec.Body.String()
	assert.NotContains(t, body, "hunter2")
	assert.NotContains(t, body, "postgres://")
	assert.Contains(t, body, "listing events failed")
}
