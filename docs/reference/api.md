# REST API

Base URL: wherever `HTTP_ADDR` serves (default `http://localhost:8080`). All API routes are under `/api`; the same paths without `/api` serve the HTML web UI. Responses are JSON. List endpoints paginate with `cursor` and `limit`.

> This documents the intended MVP surface. The authoritative reference is your build's README and code — if a request here is rejected, check there and treat the code as truth.

## Health & stats

### `GET /health`
Reports process health, including the active source (RPC or SoroTrail) and, in standalone mode, database reachability.

### `GET /api/stats`
Overview counters: total contracts seen, total events, last-updated ledger (standalone mode) or last-synced position (upstream mode).

## Contracts

### `GET /api/contracts`
List every known contract, newest-activity-first.

**Query parameters:** `search` (partial contract ID match), `limit`, `cursor`.

```bash
curl 'http://localhost:8080/api/contracts?limit=20'
```

### `GET /api/contracts/{id}`
Summary for one contract: event count, first/last seen ledger, event-type breakdown.

## Events

### `GET /api/contracts/{id}/events`
A contract's events, newest-first.

**Query parameters:**

| Param | Description |
|---|---|
| `type` | `contract`, `system`, or `diagnostic` |
| `tx_hash` | Restrict to events from one transaction |
| `topic` | Match by decoded topic (see build semantics) |
| `from_ledger` / `to_ledger` | Inclusive ledger bounds |
| `limit` / `cursor` | Pagination |

```bash
curl 'http://localhost:8080/api/contracts/CC.../events?type=contract&limit=50'
```

### `GET /api/events/{id}`
Fetch a single event by its unique id (TOID), fully decoded.

### `GET /api/events/stream`
Stream newly ingested events in real time over Server-Sent Events (`text/event-stream`).

**Query parameters:** Accepts the same filters as `GET /api/events`: `contract_id`, `type` (`contract`, `system`, `diagnostic`), `topic`, `tx_hash`, `from_ledger`, and `to_ledger`.

```bash
curl -N 'http://localhost:8080/api/events/stream?contract_id=CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC&type=contract'
```

**Frame format & heartbeats:**
- An immediate `:hb` comment frame is flushed upon connection so clients receive HTTP 200 headers right away, followed by `:hb` comments every 15 seconds to keep idle connections alive through reverse proxies.
- Matching events are emitted as `event: event` frames with `id: <TOID>` and `data: <Event JSON>`.

**Backpressure & delivery guarantees:**
- Delivery is **best-effort** and live-only (no database connection is held per subscriber).
- Each subscriber has a bounded in-memory channel of **256 events**. The ingester's `Publish` path is non-blocking: if a slow or stalled client fills its 256-event buffer, the broker closes that subscriber's stream and removes the subscription rather than stalling ingestion or growing memory without bound.
- Clients that disconnect or are dropped for backpressure can catch up without gaps by querying `GET /api/events` with a `cursor` or `from_ledger`.
- **Upstream mode (`SOURCE_MODE=sorotrail`):** Because upstream mode has no local ingester, `GET /api/events/stream` explicitly returns `501 Not Implemented` rather than holding open a silent stream.

## Pagination

List responses include `next_cursor`; pass it back as `cursor` for the next page. A null `next_cursor` means you've reached the end. Treat the cursor as opaque.

## Errors

Errors return an envelope with an HTTP status matching the failure class — `400` for invalid input (naming the offending parameter), `404` for an unknown contract or event, `500` for server faults. In upstream mode, an unreachable SoroTrail instance surfaces as a `502`-class error rather than a silent empty result.
