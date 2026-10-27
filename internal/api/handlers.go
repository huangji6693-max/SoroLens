package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/sorotrail/sorolens/internal/buildinfo"
	"github.com/sorotrail/sorolens/internal/config"
	"github.com/sorotrail/sorolens/internal/reqid"
	"github.com/sorotrail/sorolens/internal/source"
)

// errorResponse is the structured error envelope. "error" carries the
// human-readable message; "code" is a stable token clients can branch on;
// "request_id" maps a quoted error to one request in the logs (also on the
// X-Request-ID response header).
type errorResponse struct {
	Error     string `json:"error"`
	Code      string `json:"code,omitempty"`
	RequestID string `json:"request_id,omitempty"`
}

func (s *Server) handleListContracts(w http.ResponseWriter, r *http.Request) {
	q := source.ContractQuery{
		Search: r.URL.Query().Get("search"),
		Cursor: r.URL.Query().Get("cursor"),
	}

	limit, err := parseLimit(r.URL.Query().Get("limit"))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, err)
		return
	}
	q.Limit = limit

	page, err := s.src.ListContracts(r.Context(), q)
	if err != nil {
		s.fail(w, r, "listing contracts", err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) handleListEvents(w http.ResponseWriter, r *http.Request) {
	q, err := EventQueryFromRequest(r)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, err)
		return
	}

	page, err := s.src.ListEvents(r.Context(), q)
	if err != nil {
		s.fail(w, r, "listing events", err)
		return
	}
	writeJSON(w, http.StatusOK, annotatedPage{Events: annotateEvents(page.Events), NextCursor: page.NextCursor})
}

func (s *Server) handleContractEvents(w http.ResponseWriter, r *http.Request) {
	contractID := chi.URLParam(r, "id")
	if !config.ValidContractID(contractID) {
		writeError(w, r, http.StatusBadRequest, fmt.Errorf("invalid contract ID %q", contractID))
		return
	}

	q, err := EventQueryFromRequest(r)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, err)
		return
	}
	q.ContractID = contractID

	page, err := s.src.ListEvents(r.Context(), q)
	if err != nil {
		s.fail(w, r, "listing contract events", err)
		return
	}
	writeJSON(w, http.StatusOK, annotatedPage{Events: annotateEvents(page.Events), NextCursor: page.NextCursor})
}

// handleGetContract serves GET /api/contracts/{id}: one contract's summary —
// event count, ledger range and type breakdown — in the same shape the list
// endpoint's rows carry, so a client parsing one parses the other.
func (s *Server) handleGetContract(w http.ResponseWriter, r *http.Request) {
	contractID := chi.URLParam(r, "id")
	if !config.ValidContractID(contractID) {
		writeError(w, r, http.StatusBadRequest, fmt.Errorf("invalid contract ID %q", contractID))
		return
	}
	stats, err := s.src.ContractStats(r.Context(), contractID)
	if err != nil {
		s.fail(w, r, "loading contract", err)
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

func (s *Server) handleContractStats(w http.ResponseWriter, r *http.Request) {
	contractID := chi.URLParam(r, "id")
	if !config.ValidContractID(contractID) {
		writeError(w, r, http.StatusBadRequest, fmt.Errorf("invalid contract ID %q", contractID))
		return
	}

	stats, err := s.src.ContractStats(r.Context(), contractID)
	if err != nil {
		s.fail(w, r, "loading contract stats", err)
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

func (s *Server) handleGetEvent(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	event, err := s.src.GetEvent(r.Context(), id)
	if errors.Is(err, source.ErrNotFound) {
		writeError(w, r, http.StatusNotFound, fmt.Errorf("event %q not found", id))
		return
	}
	if err != nil {
		s.fail(w, r, "loading event", err)
		return
	}
	writeJSON(w, http.StatusOK, annotatedEvent{Event: event, Token: DetectTokenEvent(event)})
}

// handleVersion serves the build information baked in via -ldflags, so a
// running instance identifies the exact source it was built from.
func (s *Server) handleVersion(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, buildinfo.Info{
		Version: buildinfo.Version,
		Commit:  buildinfo.Commit,
		Date:    buildinfo.Date,
	})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.src.Stats(r.Context())
	if err != nil {
		s.fail(w, r, "loading stats", err)
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

func (s *Server) handleEventBuckets(w http.ResponseWriter, r *http.Request) {
	q, err := BucketQueryFromRequest(r)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, err)
		return
	}

	series, err := s.src.EventBuckets(r.Context(), q)
	if err != nil {
		s.fail(w, r, "loading event buckets", err)
		return
	}
	writeJSON(w, http.StatusOK, series)
}

func (s *Server) handleContractBuckets(w http.ResponseWriter, r *http.Request) {
	contractID := chi.URLParam(r, "id")
	if !config.ValidContractID(contractID) {
		writeError(w, r, http.StatusBadRequest, fmt.Errorf("invalid contract ID %q", contractID))
		return
	}

	q, err := BucketQueryFromRequest(r)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, err)
		return
	}
	q.ContractID = contractID

	series, err := s.src.EventBuckets(r.Context(), q)
	if err != nil {
		s.fail(w, r, "loading contract event buckets", err)
		return
	}
	writeJSON(w, http.StatusOK, series)
}

// BucketQueryFromRequest parses and validates time-bucketed aggregation query
// parameters: contract_id, type, topic, from, to, and interval. It enforces the
// shared MaxBuckets cap so an unauthenticated caller cannot request a tiny
// interval across a wide time span.
func BucketQueryFromRequest(r *http.Request) (source.BucketQuery, error) {
	params := r.URL.Query()
	var q source.BucketQuery

	if contractID := params.Get("contract_id"); contractID != "" {
		if !config.ValidContractID(contractID) {
			return q, fmt.Errorf("invalid contract_id %q", contractID)
		}
		q.ContractID = contractID
	}

	switch t := params.Get("type"); t {
	case "", "contract", "system", "diagnostic":
		q.Type = t
	default:
		return q, fmt.Errorf("invalid type %q (want contract, system or diagnostic)", t)
	}

	if topic := params.Get("topic"); topic != "" {
		if json.Valid([]byte(topic)) {
			q.Topic = json.RawMessage(topic)
		} else {
			encoded, err := json.Marshal(map[string]string{"symbol": topic})
			if err != nil {
				return q, fmt.Errorf("invalid topic: %w", err)
			}
			q.Topic = encoded
		}
	}

	from, err := parseTimestamp(params.Get("from"), "from")
	if err != nil {
		return q, err
	}
	to, err := parseTimestamp(params.Get("to"), "to")
	if err != nil {
		return q, err
	}
	if from.After(to) {
		return q, fmt.Errorf("from (%s) must not be after to (%s)", from.Format(time.RFC3339), to.Format(time.RFC3339))
	}
	q.From = from
	q.To = to

	interval, err := parseBucketInterval(params.Get("interval"))
	if err != nil {
		return q, err
	}
	q.Interval = interval

	span := q.To.Sub(q.From)
	buckets := int64(span / q.Interval)
	if span%q.Interval != 0 {
		buckets++
	}
	if buckets > source.MaxBuckets {
		return q, fmt.Errorf("requested range produces %d buckets, exceeding maximum of %d", buckets, source.MaxBuckets)
	}

	return q, nil
}

func parseTimestamp(raw, name string) (time.Time, error) {
	if raw == "" {
		return time.Time{}, fmt.Errorf("%s is required (RFC3339 timestamp)", name)
	}
	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s must be a valid RFC3339 timestamp", name)
	}
	return t.UTC(), nil
}

func parseBucketInterval(raw string) (time.Duration, error) {
	if raw == "" {
		return time.Hour, nil
	}
	var (
		d   time.Duration
		err error
	)
	if strings.HasSuffix(raw, "d") {
		days, parseErr := strconv.Atoi(strings.TrimSuffix(raw, "d"))
		if parseErr != nil || days <= 0 {
			return 0, fmt.Errorf("invalid interval %q", raw)
		}
		d = time.Duration(days) * 24 * time.Hour
	} else {
		d, err = time.ParseDuration(raw)
		if err != nil {
			return 0, fmt.Errorf("invalid interval %q", raw)
		}
	}
	if d < source.MinBucketInterval || d > source.MaxBucketInterval || d%time.Second != 0 {
		return 0, fmt.Errorf("interval must be between %s and %s in whole seconds",
			source.FormatBucketInterval(source.MinBucketInterval),
			source.FormatBucketInterval(source.MaxBucketInterval))
	}
	return d, nil
}

// EventQueryFromRequest parses the event filter query parameters shared by the
// JSON API and the web UI: type, topic, from_ledger, to_ledger, cursor, limit.
// It is exported so internal/web parses filters identically — a filter must
// mean the same thing in both places.
func EventQueryFromRequest(r *http.Request) (source.EventQuery, error) {
	params := r.URL.Query()
	q := source.EventQuery{Cursor: params.Get("cursor"), TxHash: params.Get("tx_hash")}

	switch t := params.Get("type"); t {
	case "", "contract", "system", "diagnostic":
		q.Type = t
	default:
		return q, fmt.Errorf("invalid type %q (want contract, system or diagnostic)", t)
	}

	// topic accepts any JSON value. A bare word such as `transfer` is treated
	// as the decoded symbol {"symbol":"transfer"}, which is what an event name
	// actually looks like once decoded — the common case should not require
	// knowing the wrapper format.
	if topic := params.Get("topic"); topic != "" {
		if json.Valid([]byte(topic)) {
			q.Topic = json.RawMessage(topic)
		} else {
			encoded, err := json.Marshal(map[string]string{"symbol": topic})
			if err != nil {
				return q, fmt.Errorf("invalid topic: %w", err)
			}
			q.Topic = encoded
		}
	}

	var err error
	if q.FromLedger, err = parseLedger(params.Get("from_ledger"), "from_ledger"); err != nil {
		return q, err
	}
	if q.ToLedger, err = parseLedger(params.Get("to_ledger"), "to_ledger"); err != nil {
		return q, err
	}
	if q.FromLedger > 0 && q.ToLedger > 0 && q.FromLedger > q.ToLedger {
		return q, fmt.Errorf("from_ledger %d is after to_ledger %d", q.FromLedger, q.ToLedger)
	}

	if q.Limit, err = parseLimit(params.Get("limit")); err != nil {
		return q, err
	}
	return q, nil
}

func parseLedger(raw, name string) (int64, error) {
	if raw == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return n, nil
}

func parseLimit(raw string) (int, error) {
	if raw == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 || n > source.MaxLimit {
		return 0, fmt.Errorf("limit must be an integer in [1,%d]", source.MaxLimit)
	}
	return n, nil
}

// fail logs the underlying error and returns a generic message, so internal
// details never leak into an API response.
func (s *Server) fail(w http.ResponseWriter, r *http.Request, action string, err error) {
	s.log.Error(action+" failed", "request_id", reqid.From(r), "error", err)
	writeError(w, r, http.StatusInternalServerError, errors.New(action+" failed"))
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, r *http.Request, status int, err error) {
	writeJSON(w, status, errorResponse{
		Error:     err.Error(),
		Code:      http.StatusText(status),
		RequestID: reqid.From(r),
	})
}
