// PostgreSQL helpers for the Restaurant service.

import ballerina/sql;
import ballerinax/postgresql;
import ballerinax/postgresql.driver as _;

configurable string DB_HOST = "localhost";
configurable int DB_PORT = 5432;
configurable string DB_NAME = "restaurant_db";
configurable string DB_USER = "postgres";
configurable string DB_PASSWORD = "postgres";

final postgresql:Client db = check initClient();

function initClient() returns postgresql:Client|error => new (
    host = DB_HOST, port = DB_PORT, database = DB_NAME,
    username = DB_USER, password = DB_PASSWORD
);

// ----- Restaurants -----
public function createRestaurant(Restaurant r) returns sql:Error? {
    sql:ParameterizedQuery q = `INSERT INTO restaurants
        (id, name, cuisine, address, opens_at, closes_at, is_open)
        VALUES (${r.id}::uuid, ${r.name}, ${r?.cuisine}, ${r?.address},
                ${r.opensAt}::time, ${r.closesAt}::time, ${r.isOpen})`;
    _ = check db->execute(q);
}

public function listRestaurants(int maxRows) returns Restaurant[]|error {
    sql:ParameterizedQuery q = `SELECT id, name, cuisine, address,
        opens_at::text, closes_at::text, is_open, created_at::text
        FROM restaurants ORDER BY created_at DESC LIMIT ${maxRows}`;
    stream<record {
        string id; string name; string? cuisine; string? address;
        string opens_at; string closes_at; boolean is_open; string created_at;
    }, sql:Error?> rs = db->query(q);
    Restaurant[] out = [];
    check from record {
        string id; string name; string? cuisine; string? address;
        string opens_at; string closes_at; boolean is_open; string created_at;
    } r in rs
        do {
            out.push({id: r.id, name: r.name, cuisine: r.cuisine,
                address: r.address, opensAt: r.opens_at, closesAt: r.closes_at,
                isOpen: r.is_open, createdAt: r.created_at});
        };
    return out;
}

public function findRestaurant(string id) returns Restaurant|ApiError|error {
    sql:ParameterizedQuery q = `SELECT id, name, cuisine, address,
        opens_at::text, closes_at::text, is_open, created_at::text
        FROM restaurants WHERE id = ${id}::uuid`;
    record {
        string id; string name; string? cuisine; string? address;
        string opens_at; string closes_at; boolean is_open; string created_at;
    }? r = check db->queryRow(q);
    if r is () {
        return <ApiError>{code: "NOT_FOUND", message: "Restaurant not found"};
    }
    return {id: r.id, name: r.name, cuisine: r.cuisine, address: r.address,
        opensAt: r.opens_at, closesAt: r.closes_at,
        isOpen: r.is_open, createdAt: r.created_at};
}

// ----- Menu items -----
public function createMenuItem(MenuItem m) returns sql:Error? {
    sql:ParameterizedQuery q = `INSERT INTO menu_items
        (id, restaurant_id, name, description, price_cents, available, stock_qty)
        VALUES (${m.id}::uuid, ${m.restaurantId}::uuid, ${m.name},
                ${m?.description}, ${m.priceCents}, ${m.available},
                ${m.stockQty})`;
    _ = check db->execute(q);
}

public function listMenu(string restaurantId) returns MenuItem[]|error {
    sql:ParameterizedQuery q = `SELECT id, restaurant_id, name, description,
        price_cents, available, stock_qty, reserved_qty
        FROM menu_items WHERE restaurant_id = ${restaurantId}::uuid
        AND available = true
        ORDER BY name`;
    stream<record {
        string id; string restaurant_id; string name; string? description;
        int price_cents; boolean available; int stock_qty; int reserved_qty;
    }, sql:Error?> rs = db->query(q);
    MenuItem[] out = [];
    check from record {
        string id; string restaurant_id; string name; string? description;
        int price_cents; boolean available; int stock_qty; int reserved_qty;
    } r in rs
        do {
            out.push({id: r.id, restaurantId: r.restaurant_id, name: r.name,
                description: r.description, priceCents: r.price_cents,
                available: r.available, stockQty: r.stock_qty,
                reservedQty: r.reserved_qty});
        };
    return out;
}

public function findMenuItem(string id) returns MenuItem|ApiError|error {
    sql:ParameterizedQuery q = `SELECT id, restaurant_id, name, description,
        price_cents, available, stock_qty, reserved_qty
        FROM menu_items WHERE id = ${id}::uuid`;
    record {
        string id; string restaurant_id; string name; string? description;
        int price_cents; boolean available; int stock_qty; int reserved_qty;
    }? r = check db->queryRow(q);
    if r is () {
        return <ApiError>{code: "NOT_FOUND", message: "Menu item not found"};
    }
    return {id: r.id, restaurantId: r.restaurant_id, name: r.name,
        description: r.description, priceCents: r.price_cents,
        available: r.available, stockQty: r.stock_qty, reservedQty: r.reserved_qty};
}

// ----- Inventory reservations with TTL -----
// Reserves stock; auto-release happens via background sweeper or compensation event.
public function reserveStock(string orderId, string menuItemId, int qty, int ttlSeconds)
    returns boolean|ApiError|error {

    sql:ParameterizedQuery q = `UPDATE menu_items
        SET reserved_qty = reserved_qty + ${qty}
        WHERE id = ${menuItemId}::uuid
          AND available = true
          AND (stock_qty - reserved_qty) >= ${qty}
        RETURNING id`;
    record {string id;}? r = check db->queryRow(q);
    if r is () {
        return <ApiError>{code: "OUT_OF_STOCK", message: "Insufficient stock"};
    }
    sql:ParameterizedQuery ins = `INSERT INTO inventory_reservations
        (id, order_id, menu_item_id, qty, expires_at)
        VALUES (gen_random_uuid(), ${orderId}::uuid,
                ${menuItemId}::uuid, ${qty},
                NOW() + (${ttlSeconds} || ' seconds')::interval)`;
    _ = check db->execute(ins);
    return true;
}

public function releaseReservation(string orderId) returns sql:Error? {
    // Find active reservations
    sql:ParameterizedQuery sel = `SELECT menu_item_id, qty FROM inventory_reservations
        WHERE order_id = ${orderId}::uuid AND released_at IS NULL`;
    stream<record {string menu_item_id; int qty;}, sql:Error?> rs = db->query(sel);

    _ = check from record {string menu_item_id; int qty;} r in rs
        do {
            sql:ParameterizedQuery upd = `UPDATE menu_items
                SET reserved_qty = GREATEST(reserved_qty - ${r.qty}, 0)
                WHERE id = ${r.menu_item_id}::uuid`;
            _ = check db->execute(upd);
        };

    sql:ParameterizedQuery rel = `UPDATE inventory_reservations
        SET released_at = NOW()
        WHERE order_id = ${orderId}::uuid AND released_at IS NULL`;
    _ = check db->execute(rel);
}

public function confirmReservation(string orderId) returns sql:Error? {
    // Convert reservation to actual stock decrement
    sql:ParameterizedQuery sel = `SELECT menu_item_id, qty FROM inventory_reservations
        WHERE order_id = ${orderId}::uuid AND released_at IS NULL`;
    stream<record {string menu_item_id; int qty;}, sql:Error?> rs = db->query(sel);

    _ = check from record {string menu_item_id; int qty;} r in rs
        do {
            sql:ParameterizedQuery upd = `UPDATE menu_items
                SET stock_qty = GREATEST(stock_qty - ${r.qty}, 0),
                    reserved_qty = GREATEST(reserved_qty - ${r.qty}, 0)
                WHERE id = ${r.menu_item_id}::uuid`;
            _ = check db->execute(upd);
        };

    sql:ParameterizedQuery rel = `UPDATE inventory_reservations
        SET released_at = NOW()
        WHERE order_id = ${orderId}::uuid AND released_at IS NULL`;
    _ = check db->execute(rel);
}

// ----- Outbox writes (transactional with aggregate) -----
public function insertOutboxEvent(EventEnvelope env) returns sql:Error? {
    sql:ParameterizedQuery q = `INSERT INTO outbox_events
        (event_id, aggregate_type, aggregate_id, topic, payload, correlation_id, schema_version)
        VALUES (${env.eventId}::uuid, ${env.aggregateType}, ${env.aggregateId},
                ${env.topic}, ${env.toJsonString()}::jsonb, ${env.correlationId}::uuid,
                ${env.schemaVersion})`;
    _ = check db->execute(q);
}

// ----- Outbox relay -----
public function fetchUnpublished(int maxRows) returns OutboxRow[]|error {
    sql:ParameterizedQuery q = `SELECT id, event_id, topic, payload::text,
        correlation_id, schema_version
        FROM outbox_events WHERE published_at IS NULL
        ORDER BY id ASC LIMIT ${maxRows} FOR UPDATE SKIP LOCKED`;
    stream<record {
        int id; string event_id; string topic; string payload; string correlation_id; int schema_version;
    }, sql:Error?> rs = db->query(q);
    OutboxRow[] out = [];
    check from record {
        int id; string event_id; string topic; string payload; string correlation_id; int schema_version;
    } r in rs
        do {
            out.push({id: r.id, eventId: r.event_id, topic: r.topic,
                payload: r.payload, correlationId: r.correlation_id,
                schemaVersion: r.schema_version});
        };
    return out;
}

public function markPublished(int id) returns sql:Error? {
    sql:ParameterizedQuery q = `UPDATE outbox_events SET published_at = NOW() WHERE id = ${id}`;
    _ = check db->execute(q);
}

// ----- Consumer idempotency -----
public function isEventProcessed(string eventId, string group) returns boolean|error {
    sql:ParameterizedQuery q = `SELECT 1 AS v FROM processed_events
        WHERE event_id = ${eventId}::uuid AND consumer_group = ${group}`;
    record {|int v;|}? r = check db->queryRow(q);
    return r is record {|int v;|};
}

public function recordProcessed(string eventId, string topic, string group)
    returns sql:Error? {
    sql:ParameterizedQuery q = `INSERT INTO processed_events (event_id, topic, consumer_group)
        VALUES (${eventId}::uuid, ${topic}, ${group})
        ON CONFLICT (event_id) DO NOTHING`;
    _ = check db->execute(q);
}

// ----- Health -----
public function healthCheck() returns boolean|error {
    sql:ParameterizedQuery q = `SELECT 1 AS v`;
    record {|int v;|}|error r = db->queryRow(q);
    return r is record {|int v;|};
}