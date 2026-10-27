-- Indexes supporting time-bucketed event count aggregations over on-chain
-- ledger close time (ledger_closed_at), both globally and per contract.
CREATE INDEX IF NOT EXISTS events_ledger_closed_at_idx
    ON events (ledger_closed_at);

CREATE INDEX IF NOT EXISTS events_contract_id_ledger_closed_at_idx
    ON events (contract_id, ledger_closed_at);
