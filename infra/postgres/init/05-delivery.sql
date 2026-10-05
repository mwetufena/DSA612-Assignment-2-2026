-- Delivery Service schema
CREATE TABLE drivers (
    id UUID PRIMARY KEY,
    full_name TEXT NOT NULL,
    phone TEXT,
    vehicle TEXT,
    is_available BOOLEAN NOT NULL DEFAULT TRUE,
    current_lat DOUBLE PRECISION,
    current_lng DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE deliveries (
    id UUID PRIMARY KEY,
    order_id UUID UNIQUE NOT NULL,
    driver_id UUID REFERENCES drivers(id),
    pickup_address TEXT NOT NULL,
    dropoff_address TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'UNASSIGNED',
    assigned_at TIMESTAMPTZ,
    picked_up_at TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    version INT NOT NULL DEFAULT 0
);
CREATE INDEX idx_deliveries_status ON deliveries(status);

CREATE TABLE outbox_events (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID UNIQUE NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    topic TEXT NOT NULL,
    payload JSONB NOT NULL,
    correlation_id UUID NOT NULL,
    schema_version INT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ
);
CREATE INDEX idx_outbox_unpublished ON outbox_events(published_at) WHERE published_at IS NULL;

CREATE TABLE idempotency_keys (
    key TEXT PRIMARY KEY,
    request_hash TEXT NOT NULL,
    response_status INT NOT NULL,
    response_body JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE processed_events (
    event_id UUID PRIMARY KEY,
    topic TEXT NOT NULL,
    consumer_group TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);