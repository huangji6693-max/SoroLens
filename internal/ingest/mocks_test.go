package ingest

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/sorotrail/sorolens/internal/rpc"
	"github.com/sorotrail/sorolens/internal/source"
	"github.com/sorotrail/sorolens/internal/store"
)

// mockRPC is a scripted rpc.Client. Each call to GetEvents pops the next
// canned response, recording the request that asked for it.
type mockRPC struct {
	mu sync.Mutex

	latest    uint32
	health    rpc.Health
	responses []rpc.GetEventsResult
	errs      []error
	requests  []rpc.GetEventsRequest
}

func (m *mockRPC) GetEvents(_ context.Context, req rpc.GetEventsRequest) (rpc.GetEventsResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.requests = append(m.requests, req)

	if len(m.errs) > 0 {
		err := m.errs[0]
		m.errs = m.errs[1:]
		if err != nil {
			return rpc.GetEventsResult{}, err
		}
	}
	if len(m.responses) == 0 {
		return rpc.GetEventsResult{LatestLedger: m.latest}, nil
	}

	res := m.responses[0]
	m.responses = m.responses[1:]
	return res, nil
}

func (m *mockRPC) GetLatestLedger(context.Context) (rpc.LatestLedger, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return rpc.LatestLedger{Sequence: m.latest}, nil
}

func (m *mockRPC) GetHealth(context.Context) (rpc.Health, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.health, nil
}

func (m *mockRPC) capturedRequests() []rpc.GetEventsRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]rpc.GetEventsRequest, len(m.requests))
	copy(out, m.requests)
	return out
}

// mockStore is an in-memory store.Store recording what the ingester wrote.
type mockStore struct {
	mu sync.Mutex

	events map[string]source.Event
	state  store.IngestState

	upsertErr error
	stateErr  error
}

func newMockStore() *mockStore {
	return &mockStore{events: make(map[string]source.Event)}
}

func (m *mockStore) UpsertEvents(_ context.Context, events []source.Event) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.upsertErr != nil {
		return 0, m.upsertErr
	}

	var inserted int64
	for _, e := range events {
		// Mirror ON CONFLICT DO NOTHING: an existing ID is not re-counted.
		if _, exists := m.events[e.ID]; exists {
			continue
		}
		m.events[e.ID] = e
		inserted++
	}
	return inserted, nil
}

func (m *mockStore) GetIngestState(context.Context) (store.IngestState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stateErr != nil {
		return store.IngestState{}, m.stateErr
	}
	return m.state, nil
}

func (m *mockStore) SaveIngestState(_ context.Context, s store.IngestState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stateErr != nil {
		return m.stateErr
	}
	m.state = s
	return nil
}

func (m *mockStore) storedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.events)
}

func (m *mockStore) stored(id string) (source.Event, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.events[id]
	return e, ok
}

func (m *mockStore) savedState() store.IngestState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state
}

// The remaining Store methods are unused by the ingester; they satisfy the
// interface so the mock can stand in for a real store.
func (m *mockStore) GetEvent(context.Context, string) (source.Event, error) {
	return source.Event{}, source.ErrNotFound
}

func (m *mockStore) QueryEvents(context.Context, source.EventQuery) ([]source.Event, string, error) {
	return nil, "", nil
}

func (m *mockStore) ListContracts(context.Context, source.ContractQuery) ([]source.Contract, string, error) {
	return nil, "", nil
}

func (m *mockStore) ContractStats(context.Context, string) (source.ContractStats, error) {
	return source.ContractStats{}, nil
}

func (m *mockStore) EventBuckets(context.Context, source.BucketQuery) (source.BucketSeries, error) {
	return source.BucketSeries{}, nil
}

func (m *mockStore) Stats(context.Context) (source.Stats, error) { return source.Stats{}, nil }
func (m *mockStore) Ping(context.Context) error                  { return nil }
func (m *mockStore) Close()                                      {}

// passthroughDecoder wraps a base64 string as a symbol, so tests can assert
// which decode path ran without constructing real XDR.
type passthroughDecoder struct{}

func (passthroughDecoder) DecodeScVal(s string) (json.RawMessage, error) {
	return json.Marshal(map[string]string{"symbol": s})
}
