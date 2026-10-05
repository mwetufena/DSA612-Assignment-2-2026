// PostgreSQL helpers for the Order service.

import ballerina/sql;
import ballerinax/postgresql;
import ballerinax/postgresql.driver as _;

configurable string DB_HOST = "localhost";
configurable int DB_PORT = 5432;
configurable string DB_NAME = "order_db";
configurable string DB_USER = "postgres";
configurable string DB_PASSWORD = "postgres";

final postgresql:Client db = check initClient();

function initClient() returns postgresql:Client|error => new (
    host = DB_HOST, port = DB_PORT, database = DB_NAME,
    username = DB_USER, password = DB_PASSWORD
);

// ----- Idempotency -----
public function getIdempotentResponse(string key, string requestHash)
    returns ApiError|CachedResponse|error {
    sql:ParameterizedQuery q = `SELECT response_status, response_body::text, request_hash
        FROM idempotency_keys WHERE key = ${key}`;
    record {int response_status; string response_body; string request_hash;}? row =
        check db->queryRow(q);
    if row is () {
        return error("NOT_FOUND");
    }
    if row.request_hash != requestHash {
        return <ApiError>{code: "IDEMPOTENCY_CONFLICT",
            message: "Key reused with different payload"};
    }
    json body = check row.response_body.fromJsonString();
    return <CachedResponse>{status: row.response_status, body};
}

public function storeIdempotentResponse(string key, string requestHash,
                                        int status, json body) returns sql:Error? {
    sql:ParameterizedQuery q = `INSERT INTO idempotency_keys
        (key, request_hash, response_status, response_body)
        VALUES (${key}, ${requestHash}, ${status}, ${body.toString()}::jsonb)
        ON CONFLICT (key) DO NOTHING`;
    _ = check db->execute(q);
}

// ----- Orders -----
// Create order with optimistic locking version=0
public function createOrderWithItems(Order o, OrderItem[] items,
                                     EventEnvelope env) returns error? {
    sql:ParameterizedQuery insOrder = `INSERT INTO orders
        (id, customer_id, restaurant_id, delivery_address_id, total_cents,
         state, saga_state, correlation_id, idempotency_key, version)
        VALUES (${o.id}::uuid, ${o.customerId}::uuid,
                ${o.restaurantId}::uuid, ${o.deliveryAddressId}::uuid,
                ${o.totalCents}, ${o.state}, ${o.sagaState},
                ${o.correlationId}::uuid,
                ${o?.idempotencyKey}, ${o.version})`;
    sql:ExecutionResult|sql:Error r1 = db->execute(insOrder);
    if r1 is sql:Error { return r1; }

    foreach OrderItem item in items {
        sql:ParameterizedQuery insItem = `INSERT INTO order_items
            (id, order_id, menu_item_id, name, qty, unit_price_cents)
            VALUES (${item.id}::uuid, ${item.orderId}::uuid,
                    ${item.menuItemId}::uuid, ${item.name},
                    ${item.qty}, ${item.unitPriceCents})`;
        sql:ExecutionResult|sql:Error ir = db->execute(insItem);
        if ir is sql:Error { return ir; }
    }

    sql:ParameterizedQuery insBox = `INSERT INTO outbox_events
        (event_id, aggregate_type, aggregate_id, topic, payload,
         correlation_id, schema_version)
        VALUES (${env.eventId}::uuid, ${env.aggregateType}, ${env.aggregateId},
                ${env.topic}, ${env.toJsonString()}::jsonb,
                ${env.correlationId}::uuid, ${env.schemaVersion})`;
    sql:ExecutionResult|sql:Error r2 = db->execute(insBox);
    if r2 is sql:Error { return r2; }
}

// Optimistic state transition; emits an outbox event.
public function transitionState(string orderId, OrderState expectedState,
                                 OrderState newState, EventEnvelope env) returns sql:Error? {
    sql:ParameterizedQuery upd = `UPDATE orders
        SET state = ${newState}, updated_at = NOW(), version = version + 1
        WHERE id = ${orderId}::uuid
          AND state = ${expectedState}
          AND version = (
              SELECT version FROM orders WHERE id = ${orderId}::uuid
          )
        RETURNING version`;
    record {int version;}? r = check db->queryRow(upd);
    if r is () {
        return error("ILLEGAL_TRANSITION_OR_STALE");
    }
    sql:ParameterizedQuery insBox = `INSERT INTO outbox_events
        (event_id, aggregate_type, aggregate_id, topic, payload,
         correlation_id, schema_version)
        VALUES (${env.eventId}::uuid, ${env.aggregateType}, ${env.aggregateId},
                ${env.topic}, ${env.toJsonString()}::jsonb,
                ${env.correlationId}::uuid, ${env.schemaVersion})`;
    sql:ExecutionResult|sql:Error r2 = db->execute(insBox);
    if r2 is sql:Error { return r2; }
}

public function findOrder(string id) returns Order|ApiError|error {
    sql:ParameterizedQuery q = `SELECT id, customer_id, restaurant_id,
        delivery_address_id, total_cents, state::text, saga_state,
        correlation_id::text, idempotency_key, version, created_at::text, updated_at::text
        FROM orders WHERE id = ${id}::uuid`;
    record {
        string id; string customer_id; string restaurant_id;
        string delivery_address_id; int total_cents; string state;
        string saga_state; string correlation_id; string? idempotency_key;
        int version; string created_at; string updated_at;
    }? r = check db->queryRow(q);
    if r is () {
        return <ApiError>{code: "NOT_FOUND", message: "Order not found"};
    }
    return {
        id: r.id,
        customerId: r.customer_id,
        restaurantId: r.restaurant_id,
        deliveryAddressId: r.delivery_address_id,
        totalCents: r.total_cents,
        state: <OrderState>r.state,
        sagaState: r.saga_state,
        correlationId: r.correlation_id,
        idempotencyKey: r.idempotency_key,
        version: r.version,
        createdAt: r.created_at,
        updatedAt: r.updated_at
    };
}

public function findOrderItems(string orderId) returns OrderItem[]|error {
    sql:ParameterizedQuery q = `SELECT id, order_id, menu_item_id, name, qty, unit_price_cents
        FROM order_items WHERE order_id = ${orderId}::uuid ORDER BY id`;
    stream<record {
        string id; string order_id; string menu_item_id; string name;
        int qty; int unit_price_cents;
    }, sql:Error?> rs = db->query(q);
    OrderItem[] out = [];
    check from record {
        string id; string order_id; string menu_item_id; string name;
        int qty; int unit_price_cents;
    } r in rs
        do {
            out.push({id: r.id, orderId: r.order_id, menuItemId: r.menu_item_id,
                name: r.name, qty: r.qty, unitPriceCents: r.unit_price_cents});
        };
    return out;
}

// ----- Outbox -----
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
    sql:ParameterizedQuery q = `INSERT INTO processed_events
        (event_id, topic, consumer_group)
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