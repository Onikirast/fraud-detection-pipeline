-- Fraud detection pipeline schema
-- Auto-applied on first Postgres container startup via docker-entrypoint-initdb.d

CREATE TABLE IF NOT EXISTS transactions (
    id              UUID PRIMARY KEY,
    user_id         TEXT NOT NULL,
    amount          NUMERIC(12, 2) NOT NULL,
    merchant_category TEXT NOT NULL,
    location        TEXT NOT NULL,
    "timestamp"     TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_transactions_user_id ON transactions (user_id);
CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON transactions ("timestamp");

-- Rolling per-user profile, updated incrementally with Welford's online
-- algorithm so we never need to recompute mean/variance from full history.
CREATE TABLE IF NOT EXISTS user_profiles (
    user_id             TEXT PRIMARY KEY,
    txn_count           BIGINT NOT NULL DEFAULT 0,
    mean_amount         DOUBLE PRECISION NOT NULL DEFAULT 0,
    m2_amount           DOUBLE PRECISION NOT NULL DEFAULT 0, -- Welford's running sum of squared diffs
    common_categories   JSONB NOT NULL DEFAULT '{}'::jsonb,  -- category -> seen count
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS flagged_events (
    id              UUID PRIMARY KEY,
    transaction_id  UUID NOT NULL REFERENCES transactions (id),
    reason          TEXT NOT NULL,       -- human-readable, for display
    rule_types      TEXT[] NOT NULL DEFAULT '{}', -- e.g. {amount_zscore,velocity} -- for aggregation (see BUILD_LOG.md, Phase 3)
    score           DOUBLE PRECISION NOT NULL,
    flagged_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_flagged_events_flagged_at ON flagged_events (flagged_at);
