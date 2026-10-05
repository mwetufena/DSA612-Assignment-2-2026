-- Order Service schema
-- Order state machine: CREATED -> CONFIRMED -> PREPARING -> READY -> OUT_FOR_DELIVERY -> DELIVERED
--                                   \-> CANCELLED (from CREATED/CONFIRMED only)
CREATE TYPE order_state AS ENUM (
    'CREATED','CONFIRMED','PREPARING','READY','OUT_FOR_DELIVERY','DELIVERED','CANCELLED','REJECTED'
);

CREATE TABLE orders (
    id UUID PRIMARY KEY,
    customer_id UUID NOT NULL,
    restaurant_id UUID NOT NULL,
    delivery_address_id UUID NOT NULL,
    total_cents INT NOT NULL CHECK (total_cents >= 0),
    state order_state NOT NULL DEFAULT 'CREATED',
    saga_state TEXT NOT NULL DEFAULT 'STARTED',
    correlation_id UUID NOT NULL,
    idempotency_key TEXT UNIQUE,
    version INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_orders_state ON orders(state);

CREATE TABLE order_items (
    id UUID PRIMARY KEY,
    order_id UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    menu_item_id UUID NOT NULL,
    name TEXT NOT NULL,
    qty INT NOT NULL CHECK (qty > 0),
    unit_price_cents INT NOT NULL CHECK (unit_price_cents >= 0)
);
CREATE INDEX idx_orderitems_order ON order_items(order_id);

-- Saga step tracking for distributed transaction orchestrator
CREATE TABLE saga_steps (
    id BIGSERIAL PRIMARY KEY,
    order_id UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    step_name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    payload JSONB,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(order_id, step_name)
);

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

-- Idempotency table for Kafka consumers (eventId dedupe)
CREATE TABLE processed_events (
    event_id UUID PRIMARY KEY,
    topic TEXT NOT NULL,
    consumer_group TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_processed_topic ON processed_events(topic, consumer_group);