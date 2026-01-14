ALTER TABLE events -- Add idempotency support for safe retries
ADD COLUMN idempotency_key TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_events_source_idempotency -- Enforce uniqueness per source + idempotency key
ON events (source, idempotency_key)
WHERE idempotency_key IS NOT NULL;
