-- Admin Service schema (read-model / mini-CQRS projections)
CREATE TABLE order_stats_daily (
    day DATE PRIMARY KEY,
    restaurant_id UUID NOT NULL,
    order_count INT NOT NULL DEFAULT 0,
    delivered_count INT NOT NULL DEFAULT 0,
    cancelled_count INT NOT NULL DEFAULT 0,
    revenue_cents BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE delivery_stats_daily (
    day DATE PRIMARY KEY,
    driver_id UUID NOT NULL,
    assigned_count INT NOT NULL DEFAULT 0,
    delivered_count INT NOT NULL DEFAULT 0,
    avg_delivery_minutes DOUBLE PRECISION,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE processed_events (
    event_id UUID PRIMARY KEY,
    topic TEXT NOT NULL,
    consumer_group TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);