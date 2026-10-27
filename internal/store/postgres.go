package store

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sorotrail/sorolens/internal/source"
)

// Postgres implements Store on a pgx connection pool.
type Postgres struct {
	pool *pgxpool.Pool
}

// NewPostgres connects to Postgres and verifies the connection before
// returning, so a bad DATABASE_URL fails at startup rather than on first
// request.
func NewPostgres(ctx context.Context, databaseURL string) (*Postgres, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("connecting to postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pinging postgres: %w", err)
	}
	return &Postgres{pool: pool}, nil
}

// Close releases the connection pool.
func (p *Postgres) Close() { p.pool.Close() }

// Ping checks that the database is reachable.
func (p *Postgres) Ping(ctx context.Context) error { return p.pool.Ping(ctx) }

const eventColumns = `id, contract_id, ledger, type, tx_hash, tx_index, op_index,
	in_successful_call, topics, value, ledger_closed_at, created_at`

// UpsertEvents inserts events keyed on ID. Conflicts are ignored rather than
// updated: an event is immutable once written, so re-reading a ledger range
// after a restart is a no-op instead of a rewrite.
func (p *Postgres) UpsertEvents(ctx context.Context, events []source.Event) (int64, error) {
	if len(events) == 0 {
		return 0, nil
	}

	batch := &pgx.Batch{}
	for _, e := range events {
		topics := e.Topics
		if len(topics) == 0 {
			topics = []byte("[]")
		}
		value := e.Value
		if len(value) == 0 {
			value = []byte("null")
		}
		closedAt := e.LedgerClosedAt
		if closedAt.IsZero() {
			closedAt = time.Now().UTC()
		}

		batch.Queue(`
			INSERT INTO events (`+eventColumns+`)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, now())
			ON CONFLICT (id) DO NOTHING`,
			e.ID, e.ContractID, e.Ledger, e.Type, e.TxHash, e.TxIndex, e.OpIndex,
			e.InSuccessfulCall, topics, value, closedAt)
	}

	results := p.pool.SendBatch(ctx, batch)

	var inserted int64
	for range events {
		tag, err := results.Exec()
		if err != nil {
			_ = results.Close()
			return inserted, fmt.Errorf("inserting events: %w", err)
		}
		inserted += tag.RowsAffected()
	}

	// Closing the batch reports errors that the individual Execs did not, so
	// the count is only trustworthy once this succeeds.
	if err := results.Close(); err != nil {
		return inserted, fmt.Errorf("completing event insert: %w", err)
	}
	return inserted, nil
}

// GetEvent returns one event by TOID.
func (p *Postgres) GetEvent(ctx context.Context, id string) (source.Event, error) {
	row := p.pool.QueryRow(ctx, `SELECT `+eventColumns+` FROM events WHERE id = $1`, id)
	e, err := scanEvent(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return source.Event{}, source.ErrNotFound
	}
	if err != nil {
		return source.Event{}, fmt.Errorf("loading event %s: %w", id, err)
	}
	return e, nil
}

// QueryEvents returns a page of events newest first. The cursor is the ID of
// the last event on the previous page; because TOIDs are zero-padded, "older
// than the cursor" is simply id < cursor.
func (p *Postgres) QueryEvents(ctx context.Context, q source.EventQuery) ([]source.Event, string, error) {
	limit := source.NormalizeLimit(q.Limit)

	var (
		where []string
		args  []any
	)
	arg := func(v any) string {
		args = append(args, v)
		return "$" + strconv.Itoa(len(args))
	}

	if q.ContractID != "" {
		where = append(where, "contract_id = "+arg(q.ContractID))
	}
	if q.Type != "" {
		where = append(where, "type = "+arg(q.Type))
	}
	if q.TxHash != "" {
		where = append(where, "tx_hash = "+arg(q.TxHash))
	}
	if len(q.Topic) > 0 {
		// Containment against a one-element array asks "is this value among
		// the topics", which the GIN index on topics can serve.
		where = append(where, "topics @> "+arg([]byte("["+string(q.Topic)+"]")))
	}
	if q.FromLedger > 0 {
		where = append(where, "ledger >= "+arg(q.FromLedger))
	}
	if q.ToLedger > 0 {
		where = append(where, "ledger <= "+arg(q.ToLedger))
	}
	if q.Cursor != "" {
		where = append(where, "id < "+arg(q.Cursor))
	}

	query := `SELECT ` + eventColumns + ` FROM events`
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	// Fetch one extra row to learn whether a further page exists without a
	// second count query.
	query += " ORDER BY id DESC LIMIT " + arg(limit+1)

	rows, err := p.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, "", fmt.Errorf("querying events: %w", err)
	}
	defer rows.Close()

	events := make([]source.Event, 0, limit)
	for rows.Next() {
		e, err := scanEvent(rows)
		if err != nil {
			return nil, "", fmt.Errorf("scanning event: %w", err)
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("reading events: %w", err)
	}

	var next string
	if len(events) > limit {
		events = events[:limit]
		next = events[len(events)-1].ID
	}
	return events, next, nil
}

// ListContracts aggregates stored events per contract, most recently active
// first.
func (p *Postgres) ListContracts(ctx context.Context, q source.ContractQuery) ([]source.Contract, string, error) {
	limit := source.NormalizeLimit(q.Limit)

	var (
		where  []string
		having []string
		args   []any
	)
	arg := func(v any) string {
		args = append(args, v)
		return "$" + strconv.Itoa(len(args))
	}

	if q.Search != "" {
		// Contract IDs are uppercase strkey; upper() keeps the search
		// case-insensitive without a functional index.
		where = append(where, "contract_id LIKE "+arg("%"+strings.ToUpper(q.Search)+"%"))
	}

	if q.Cursor != "" {
		ledger, contractID, err := decodeContractCursor(q.Cursor)
		if err != nil {
			return nil, "", err
		}
		// Continue the (last_ledger DESC, contract_id ASC) ordering.
		having = append(having, fmt.Sprintf(
			"(MAX(ledger) < %s OR (MAX(ledger) = %s AND contract_id > %s))",
			arg(ledger), arg(ledger), arg(contractID)))
	}

	query := `
		SELECT contract_id,
		       COUNT(*)              AS event_count,
		       MIN(ledger)           AS first_ledger,
		       MAX(ledger)           AS last_ledger,
		       MAX(ledger_closed_at) AS last_activity
		FROM events`
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	query += " GROUP BY contract_id"
	if len(having) > 0 {
		query += " HAVING " + strings.Join(having, " AND ")
	}
	query += " ORDER BY last_ledger DESC, contract_id ASC LIMIT " + arg(limit+1)

	rows, err := p.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, "", fmt.Errorf("listing contracts: %w", err)
	}
	defer rows.Close()

	contracts := make([]source.Contract, 0, limit)
	for rows.Next() {
		var c source.Contract
		if err := rows.Scan(&c.ID, &c.EventCount, &c.FirstLedger, &c.LastLedger, &c.LastActivity); err != nil {
			return nil, "", fmt.Errorf("scanning contract: %w", err)
		}
		contracts = append(contracts, c)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("reading contracts: %w", err)
	}

	var next string
	if len(contracts) > limit {
		contracts = contracts[:limit]
		last := contracts[len(contracts)-1]
		next = encodeContractCursor(last.LastLedger, last.ID)
	}
	return contracts, next, nil
}

// ContractStats summarizes one contract's stored events.
func (p *Postgres) ContractStats(ctx context.Context, contractID string) (source.ContractStats, error) {
	out := source.ContractStats{ContractID: contractID}

	row := p.pool.QueryRow(ctx, `
		SELECT COUNT(*), COALESCE(MIN(ledger), 0), COALESCE(MAX(ledger), 0)
		FROM events WHERE contract_id = $1`, contractID)
	if err := row.Scan(&out.TotalEvents, &out.FirstLedger, &out.LastLedger); err != nil {
		return out, fmt.Errorf("loading contract stats: %w", err)
	}

	breakdown, err := p.typeBreakdown(ctx, "WHERE contract_id = $1", contractID)
	if err != nil {
		return out, err
	}
	out.TypeBreakdown = breakdown
	return out, nil
}

// EventBuckets aggregates stored event counts into contiguous half-open
// [Start, End) time buckets over [q.From, q.To) using on-chain ledger close
// time (ledger_closed_at). Empty buckets are generated in SQL via
// generate_series so quiet periods are preserved with a zero count.
func (p *Postgres) EventBuckets(ctx context.Context, q source.BucketQuery) (source.BucketSeries, error) {
	from := q.From.UTC()
	to := q.To.UTC()
	out := source.BucketSeries{
		Interval:    source.FormatBucketInterval(q.Interval),
		From:        from,
		To:          to,
		Buckets:     []source.EventBucket{},
		Approximate: false,
	}
	if !from.Before(to) || q.Interval <= 0 {
		return out, nil
	}

	args := []any{
		from,
		to,
		fmt.Sprintf("%d seconds", int64(q.Interval/time.Second)),
	}
	arg := func(v any) string {
		args = append(args, v)
		return "$" + strconv.Itoa(len(args))
	}

	joinPredicates := []string{
		"e.ledger_closed_at >= s.bucket_start",
		"e.ledger_closed_at < s.bucket_end",
	}
	if q.ContractID != "" {
		joinPredicates = append(joinPredicates, "e.contract_id = "+arg(q.ContractID))
	}
	if q.Type != "" {
		joinPredicates = append(joinPredicates, "e.type = "+arg(q.Type))
	}
	if len(q.Topic) > 0 {
		joinPredicates = append(joinPredicates, "e.topics @> "+arg([]byte("["+string(q.Topic)+"]")))
	}

	query := `
		WITH series AS (
			SELECT gs AS bucket_start,
			       LEAST(gs + $3::interval, $2::timestamptz) AS bucket_end
			FROM generate_series($1::timestamptz, $2::timestamptz, $3::interval) AS gs
			WHERE gs < $2::timestamptz
		)
		SELECT s.bucket_start,
		       s.bucket_end,
		       COUNT(e.id) AS event_count
		FROM series s
		LEFT JOIN events e ON ` + strings.Join(joinPredicates, " AND ") + `
		GROUP BY s.bucket_start, s.bucket_end
		ORDER BY s.bucket_start ASC`

	rows, err := p.pool.Query(ctx, query, args...)
	if err != nil {
		return out, fmt.Errorf("querying event buckets: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var b source.EventBucket
		if err := rows.Scan(&b.Start, &b.End, &b.Count); err != nil {
			return out, fmt.Errorf("scanning event bucket: %w", err)
		}
		b.Start = b.Start.UTC()
		b.End = b.End.UTC()
		out.Buckets = append(out.Buckets, b)
		out.TotalEvents += b.Count
	}
	if err := rows.Err(); err != nil {
		return out, fmt.Errorf("reading event buckets: %w", err)
	}
	return out, nil
}

// Stats summarizes everything stored.
func (p *Postgres) Stats(ctx context.Context) (source.Stats, error) {
	var out source.Stats

	row := p.pool.QueryRow(ctx, `
		SELECT COUNT(*), COUNT(DISTINCT contract_id),
		       COALESCE(MIN(ledger), 0), COALESCE(MAX(ledger), 0)
		FROM events`)
	if err := row.Scan(&out.TotalEvents, &out.ContractCount, &out.FirstLedger, &out.LastLedger); err != nil {
		return out, fmt.Errorf("loading stats: %w", err)
	}

	breakdown, err := p.typeBreakdown(ctx, "")
	if err != nil {
		return out, err
	}
	out.TypeBreakdown = breakdown
	return out, nil
}

func (p *Postgres) typeBreakdown(ctx context.Context, whereClause string, args ...any) ([]source.TypeCount, error) {
	query := "SELECT type, COUNT(*) FROM events " + whereClause + " GROUP BY type ORDER BY COUNT(*) DESC, type"
	rows, err := p.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("loading type breakdown: %w", err)
	}
	defer rows.Close()

	var out []source.TypeCount
	for rows.Next() {
		var tc source.TypeCount
		if err := rows.Scan(&tc.Type, &tc.Count); err != nil {
			return nil, fmt.Errorf("scanning type breakdown: %w", err)
		}
		out = append(out, tc)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reading type breakdown: %w", err)
	}
	return out, nil
}

// GetIngestState reads the single ingestion-progress row.
func (p *Postgres) GetIngestState(ctx context.Context) (IngestState, error) {
	var s IngestState
	row := p.pool.QueryRow(ctx,
		`SELECT last_ledger, last_cursor, updated_at FROM ingest_state WHERE id = 1`)
	if err := row.Scan(&s.LastLedger, &s.LastCursor, &s.UpdatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// The migration seeds this row; treat a missing one as a cold
			// start rather than a hard failure.
			return IngestState{}, nil
		}
		return s, fmt.Errorf("loading ingest state: %w", err)
	}
	return s, nil
}

// SaveIngestState records ingestion progress.
func (p *Postgres) SaveIngestState(ctx context.Context, s IngestState) error {
	_, err := p.pool.Exec(ctx, `
		INSERT INTO ingest_state (id, last_ledger, last_cursor, updated_at)
		VALUES (1, $1, $2, now())
		ON CONFLICT (id) DO UPDATE
		SET last_ledger = EXCLUDED.last_ledger,
		    last_cursor = EXCLUDED.last_cursor,
		    updated_at  = EXCLUDED.updated_at`,
		s.LastLedger, s.LastCursor)
	if err != nil {
		return fmt.Errorf("saving ingest state: %w", err)
	}
	return nil
}

// rowScanner covers both pgx.Row and pgx.Rows, so scanEvent serves the
// single-row and multi-row paths alike.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanEvent(s rowScanner) (source.Event, error) {
	var e source.Event
	err := s.Scan(&e.ID, &e.ContractID, &e.Ledger, &e.Type, &e.TxHash, &e.TxIndex,
		&e.OpIndex, &e.InSuccessfulCall, &e.Topics, &e.Value, &e.LedgerClosedAt, &e.CreatedAt)
	return e, err
}

// Contract cursors carry both sort keys, since contract_id alone cannot
// resume an ordering led by last_ledger. They are opaque to callers.
func encodeContractCursor(ledger int64, contractID string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(ledger, 10) + "|" + contractID))
}

func decodeContractCursor(cursor string) (int64, string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, "", fmt.Errorf("invalid cursor")
	}
	ledgerStr, contractID, ok := strings.Cut(string(raw), "|")
	if !ok {
		return 0, "", fmt.Errorf("invalid cursor")
	}
	ledger, err := strconv.ParseInt(ledgerStr, 10, 64)
	if err != nil {
		return 0, "", fmt.Errorf("invalid cursor")
	}
	return ledger, contractID, nil
}
