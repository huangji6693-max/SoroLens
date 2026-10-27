package api

import (
	"bytes"
	"encoding/json"
	"sync"

	"github.com/sorotrail/sorolens/internal/source"
)

// DefaultSubscriberBufferSize bounds the event channel per SSE subscriber.
// When a slow or stalled subscriber's buffer fills up, Publish drops and
// disconnects that subscriber rather than blocking the ingester or growing
// memory without bound.
const DefaultSubscriberBufferSize = 256

// Broker fans out newly ingested events to active SSE subscribers whose
// filters match. A nil *Broker is safe: all methods no-op.
type Broker struct {
	mu           sync.RWMutex
	subscribers  map[int64]*subscriber
	nextID       int64
	bufSize      int
	closed       bool
}

type subscriber struct {
	id     int64
	filter source.EventQuery
	ch     chan source.Event
}

// NewBroker constructs a Broker with DefaultSubscriberBufferSize.
func NewBroker() *Broker {
	return NewBrokerWithBuffer(DefaultSubscriberBufferSize)
}

// NewBrokerWithBuffer constructs a Broker with a caller-specified per-subscriber
// channel capacity. Values <= 0 fall back to DefaultSubscriberBufferSize.
func NewBrokerWithBuffer(bufSize int) *Broker {
	if bufSize <= 0 {
		bufSize = DefaultSubscriberBufferSize
	}
	return &Broker{
		subscribers: make(map[int64]*subscriber),
		bufSize:     bufSize,
	}
}

// Subscribe registers a subscriber with the given EventQuery filter and returns
// a receive-only event channel plus an idempotent cancel function. When the
// subscriber's bounded buffer overflows or Shutdown is called, the channel is
// closed automatically.
func (b *Broker) Subscribe(filter source.EventQuery) (<-chan source.Event, func()) {
	if b == nil {
		ch := make(chan source.Event)
		close(ch)
		return ch, func() {}
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		ch := make(chan source.Event)
		close(ch)
		return ch, func() {}
	}

	id := b.nextID
	b.nextID++
	sub := &subscriber{
		id:     id,
		filter: filter,
		ch:     make(chan source.Event, b.bufSize),
	}
	b.subscribers[id] = sub

	cancel := func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		if s, ok := b.subscribers[id]; ok {
			close(s.ch)
			delete(b.subscribers, id)
		}
	}
	return sub.ch, cancel
}

// Publish delivers matching events to every active subscriber without blocking.
// Any subscriber whose buffer is full is immediately disconnected (its channel
// is closed and its subscription is removed) so a slow consumer can never block
// ingestion or accumulate unbounded memory.
func (b *Broker) Publish(events []source.Event) {
	if b == nil || len(events) == 0 {
		return
	}

	b.mu.RLock()
	if b.closed || len(b.subscribers) == 0 {
		b.mu.RUnlock()
		return
	}

	var dropped []int64
	for _, s := range b.subscribers {
		for _, e := range events {
			if !eventMatchesQuery(s.filter, e) {
				continue
			}
			select {
			case s.ch <- e:
			default:
				dropped = append(dropped, s.id)
				goto nextSubscriber
			}
		}
	nextSubscriber:
	}
	b.mu.RUnlock()

	if len(dropped) > 0 {
		b.mu.Lock()
		for _, id := range dropped {
			if s, ok := b.subscribers[id]; ok {
				close(s.ch)
				delete(b.subscribers, id)
			}
		}
		b.mu.Unlock()
	}
}

// Shutdown closes all active subscriber channels and prevents new subscriptions,
// allowing in-flight SSE handlers to exit cleanly during server shutdown.
func (b *Broker) Shutdown() {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	for id, s := range b.subscribers {
		close(s.ch)
		delete(b.subscribers, id)
	}
}

// SubscriberCount reports the number of currently registered subscribers.
func (b *Broker) SubscriberCount() int {
	if b == nil {
		return 0
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subscribers)
}

func eventMatchesQuery(q source.EventQuery, e source.Event) bool {
	if q.ContractID != "" && e.ContractID != q.ContractID {
		return false
	}
	if q.Type != "" && e.Type != q.Type {
		return false
	}
	if q.TxHash != "" && e.TxHash != q.TxHash {
		return false
	}
	if q.FromLedger > 0 && e.Ledger < q.FromLedger {
		return false
	}
	if q.ToLedger > 0 && e.Ledger > q.ToLedger {
		return false
	}
	if len(q.Topic) > 0 && !topicMatchesEvent(e.Topics, q.Topic) {
		return false
	}
	return true
}

func topicMatchesEvent(topics, wantTopic json.RawMessage) bool {
	want := compactTopicJSON(wantTopic)
	if want == "" || len(topics) == 0 {
		return false
	}
	var elements []json.RawMessage
	if err := json.Unmarshal(topics, &elements); err != nil {
		return false
	}
	for _, elem := range elements {
		if compactTopicJSON(elem) == want {
			return true
		}
	}
	return false
}

func compactTopicJSON(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return ""
	}
	return buf.String()
}
