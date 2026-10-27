package api

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sorotrail/sorolens/internal/metrics"
	"github.com/sorotrail/sorolens/internal/source"
)

const secondTestContract = "CAS3J7GYLGXMF6TDJBBYYSE3HQ6BBSMLNUQ34T6TZMYMW2SLH34INTBP"

func testStreamServer(src source.EventSource, broker *Broker, hb time.Duration) *httptest.Server {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := New(src, log).WithBroker(broker).WithHeartbeatInterval(hb)
	m := metrics.New()
	metrics.RoutePattern = func(req *http.Request) string {
		if rc := chi.RouteContext(req.Context()); rc != nil {
			return rc.RoutePattern()
		}
		return ""
	}

	r := chi.NewRouter()
	r.Use(m.Middleware)
	r.Mount("/api", s.Routes())
	return httptest.NewServer(r)
}

func waitForSubscribers(t *testing.T, b *Broker, want int) {
	t.Helper()
	require.Eventually(t, func() bool {
		return b.SubscriberCount() == want
	}, 2*time.Second, 10*time.Millisecond, "subscriber count did not reach %d", want)
}

func TestEventStreamSubscriberReceivesMatchingEventsAndDisconnectCleansUp(t *testing.T) {
	src := &fakeSource{status: source.Status{Mode: "rpc", Healthy: true}}
	broker := NewBroker()
	ts := testStreamServer(src, broker, 30*time.Millisecond)
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	url := ts.URL + "/api/events/stream?contract_id=" + testContract + "&type=contract&topic=transfer"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))

	waitForSubscribers(t, broker, 1)

	reader := bufio.NewReader(resp.Body)
	// First frame is the immediate :hb comment.
	firstLine, err := reader.ReadString('\n')
	require.NoError(t, err)
	assert.Equal(t, ":hb\n", firstLine)
	_, err = reader.ReadString('\n')
	require.NoError(t, err)

	// Publish a non-matching contract event, a non-matching topic event, and a matching event.
	broker.Publish([]source.Event{
		{
			ID:         "0000000100-0000000001",
			ContractID: secondTestContract,
			Ledger:     100,
			Type:       "contract",
			Topics:     json.RawMessage(`[{"symbol":"transfer"}]`),
		},
		{
			ID:         "0000000100-0000000002",
			ContractID: testContract,
			Ledger:     100,
			Type:       "contract",
			Topics:     json.RawMessage(`[{"symbol":"mint"}]`),
		},
		{
			ID:         "0000000101-0000000001",
			ContractID: testContract,
			Ledger:     101,
			Type:       "contract",
			Topics:     json.RawMessage(`[{"symbol":"transfer"}]`),
			Value:      json.RawMessage(`{"i128":"500"}`),
		},
	})

	// Read until we see the data line for the matching event.
	var idLine, eventLine, dataLine string
	for {
		line, err := reader.ReadString('\n')
		require.NoError(t, err)
		line = strings.TrimRight(line, "\r\n")
		if strings.HasPrefix(line, "id: ") {
			idLine = strings.TrimPrefix(line, "id: ")
		} else if strings.HasPrefix(line, "event: ") {
			eventLine = strings.TrimPrefix(line, "event: ")
		} else if strings.HasPrefix(line, "data: ") {
			dataLine = strings.TrimPrefix(line, "data: ")
			break
		}
	}

	assert.Equal(t, "0000000101-0000000001", idLine)
	assert.Equal(t, "event", eventLine)

	var got source.Event
	require.NoError(t, json.Unmarshal([]byte(dataLine), &got))
	assert.Equal(t, "0000000101-0000000001", got.ID)
	assert.Equal(t, testContract, got.ContractID)

	// Disconnecting the client must remove the subscriber and exit the handler goroutine.
	cancel()
	waitForSubscribers(t, broker, 0)
}

func TestEventStreamSlowConsumerIsDroppedWithoutBlockingPublish(t *testing.T) {
	broker := NewBrokerWithBuffer(3)
	ch, cancel := broker.Subscribe(source.EventQuery{ContractID: testContract})
	defer cancel()

	require.Equal(t, 1, broker.SubscriberCount())

	// Publish 3 events to fill the buffer.
	for i := 1; i <= 3; i++ {
		broker.Publish([]source.Event{{
			ID:         "0000000100-000000000" + string(rune('0'+i)),
			ContractID: testContract,
			Ledger:     100,
			Type:       "contract",
		}})
	}
	require.Equal(t, 1, broker.SubscriberCount())

	// Publishing a 4th event when the subscriber has not read anything must not
	// block and must drop + close the stalled subscriber.
	done := make(chan struct{})
	go func() {
		broker.Publish([]source.Event{{
			ID:         "0000000100-0000000004",
			ContractID: testContract,
			Ledger:     100,
			Type:       "contract",
		}})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Publish blocked on a full subscriber buffer")
	}

	assert.Equal(t, 0, broker.SubscriberCount())

	// Drain buffered items and confirm the channel is closed.
	for range 3 {
		_, ok := <-ch
		require.True(t, ok)
	}
	_, open := <-ch
	assert.False(t, open, "overflowed subscriber channel must be closed")
}

func TestEventStreamShutdownTerminatesActiveStreams(t *testing.T) {
	src := &fakeSource{status: source.Status{Mode: "rpc", Healthy: true}}
	broker := NewBroker()
	ts := testStreamServer(src, broker, time.Second)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/events/stream")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	waitForSubscribers(t, broker, 1)

	broker.Shutdown()
	waitForSubscribers(t, broker, 0)

	// Reading the rest of the body must reach EOF promptly once Shutdown closes
	// the subscriber channel.
	readDone := make(chan error, 1)
	go func() {
		_, err := io.ReadAll(resp.Body)
		readDone <- err
	}()

	select {
	case err := <-readDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("active SSE stream did not terminate after broker.Shutdown()")
	}
}

func TestEventStreamUpstreamModeAndValidationErrors(t *testing.T) {
	t.Run("upstream sorotrail mode returns 501 Not Implemented", func(t *testing.T) {
		src := &fakeSource{status: source.Status{Mode: "sorotrail", Healthy: true}}
		rec := do(t, testServer(src), "/api/events/stream")
		require.Equal(t, http.StatusNotImplemented, rec.Code)
		assert.Contains(t, rec.Body.String(), "unavailable in upstream mode")
	})

	t.Run("invalid contract_id returns 400 Bad Request", func(t *testing.T) {
		src := &fakeSource{status: source.Status{Mode: "rpc", Healthy: true}}
		log := slog.New(slog.NewTextHandler(io.Discard, nil))
		s := New(src, log).WithBroker(NewBroker())
		r := chi.NewRouter()
		r.Mount("/api", s.Routes())

		rec := do(t, r, "/api/events/stream?contract_id=invalid-contract")
		require.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "invalid contract ID")
	})

	t.Run("invalid type returns 400 Bad Request", func(t *testing.T) {
		src := &fakeSource{status: source.Status{Mode: "rpc", Healthy: true}}
		log := slog.New(slog.NewTextHandler(io.Discard, nil))
		s := New(src, log).WithBroker(NewBroker())
		r := chi.NewRouter()
		r.Mount("/api", s.Routes())

		rec := do(t, r, "/api/events/stream?type=unknown")
		require.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "invalid type")
	})
}
