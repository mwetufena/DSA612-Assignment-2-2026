import ballerina/sql;
import ballerinax/postgresql;
import ballerinax/postgresql.driver as _;

configurable string DB_HOST = "localhost";
configurable string DB_NAME = "delivery_db";
configurable string DB_USER = "postgres";
configurable string DB_PASSWORD = "postgres";

final postgresql:Client db = check new (
    host = DB_HOST, database = DB_NAME, username = DB_USER, password = DB_PASSWORD);

public function createDriver(Driver d) returns sql:Error? {
    sql:ParameterizedQuery q = `INSERT INTO drivers
        (id, full_name, phone, vehicle, is_available)
        VALUES (${d.id}::uuid, ${d.fullName}, ${d?.phone}, ${d?.vehicle},
                ${d.isAvailable})`;
    _ = check db->execute(q);
}

public function listDrivers() returns Driver[]|error {
    sql:ParameterizedQuery q = `SELECT id, full_name, phone, vehicle, is_available,
        created_at::text FROM drivers ORDER BY created_at DESC`;
    stream<record {
        string id; string full_name; string? phone; string? vehicle;
        boolean is_available; string created_at;
    }, sql:Error?> rs = db->query(q);
    Driver[] out = [];
    check from record {
        string id; string full_name; string? phone; string? vehicle;
        boolean is_available; string created_at;
    } r in rs
        do {
            out.push({id: r.id, fullName: r.full_name, phone: r.phone,
                vehicle: r.vehicle, isAvailable: r.is_available,
                createdAt: r.created_at});
        };
    return out;
}

public function findAvailableDriver() returns string|error {
    sql:ParameterizedQuery q = `SELECT id FROM drivers
        WHERE is_available = true ORDER BY created_at LIMIT 1`;
    record {string id;}? row = check db->queryRow(q);
    if row is () { return error("NO_DRIVER_AVAILABLE"); }
    return row.id;
}

public function markDriverBusy(string driverId) returns sql:Error? {
    sql:ParameterizedQuery q = `UPDATE drivers SET is_available = false
        WHERE id = ${driverId}::uuid`;
    _ = check db->execute(q);
}

public function markDriverAvailable(string driverId) returns sql:Error? {
    sql:ParameterizedQuery q = `UPDATE drivers SET is_available = true
        WHERE id = ${driverId}::uuid`;
    _ = check db->execute(q);
}

public function createDeliveryWithEvent(Delivery d, EventEnvelope env) returns sql:Error? {
    sql:ParameterizedQuery q = `INSERT INTO deliveries
        (id, order_id, driver_id, pickup_address, dropoff_address, status,
         assigned_at)
        VALUES (${d.id}::uuid, ${d.orderId}::uuid, ${d?.driverId}::uuid,
                ${d.pickupAddress}, ${d.dropoffAddress}, ${d.status},
                ${d?.assignedAt}::timestamptz)`;
    sql:ExecutionResult|sql:Error r = db->execute(q);
    if r is sql:Error { return r; }
    sql:ParameterizedQuery qb = `INSERT INTO outbox_events
        (event_id, aggregate_type, aggregate_id, topic, payload,
         correlation_id, schema_version)
        VALUES (${env.eventId}::uuid, ${env.aggregateType}, ${env.aggregateId},
                ${env.topic}, ${env.toJsonString()}::jsonb,
                ${env.correlationId}::uuid, ${env.schemaVersion})`;
    _ = check db->execute(qb);
}

public function findDelivery(string id) returns Delivery|ApiError|error {
    sql:ParameterizedQuery q = `SELECT id, order_id::text, driver_id::text,
        pickup_address, dropoff_address, status, created_at::text,
        assigned_at::text, picked_up_at::text, delivered_at::text
        FROM deliveries WHERE id = ${id}::uuid`;
    record {
        string id; string order_id; string? driver_id;
        string pickup_address; string dropoff_address; string status;
        string created_at; string? assigned_at; string? picked_up_at; string? delivered_at;
    }? r = check db->queryRow(q);
    if r is () { return <ApiError>{code: "NOT_FOUND", message: "Delivery not found"}; }
    return {id: r.id, orderId: r.order_id, driverId: r.driver_id,
        pickupAddress: r.pickup_address, dropoffAddress: r.dropoff_address,
        status: r.status, createdAt: r.created_at,
        assignedAt: r.assigned_at, pickedUpAt: r.picked_up_at,
        deliveredAt: r.delivered_at};
}

public function findDeliveryByOrder(string orderId) returns Delivery?|error {
    sql:ParameterizedQuery q = `SELECT id, order_id::text, driver_id::text,
        pickup_address, dropoff_address, status, created_at::text,
        assigned_at::text, picked_up_at::text, delivered_at::text
        FROM deliveries WHERE order_id = ${orderId}::uuid`;
    record {
        string id; string order_id; string? driver_id;
        string pickup_address; string dropoff_address; string status;
        string created_at; string? assigned_at; string? picked_up_at; string? delivered_at;
    }? r = check db->queryRow(q);
    if r is () { return (); }
    return {id: r.id, orderId: r.order_id, driverId: r.driver_id,
        pickupAddress: r.pickup_address, dropoffAddress: r.dropoff_address,
        status: r.status, createdAt: r.created_at,
        assignedAt: r.assigned_at, pickedUpAt: r.picked_up_at,
        deliveredAt: r.delivered_at};
}

public function updateDeliveryStatus(string deliveryId, string status,
                                      EventEnvelope env) returns sql:Error? {
    string updateClause = "status = " + status;
    if status == "PICKED_UP" {
        sql:ParameterizedQuery q = `UPDATE deliveries SET status = ${status},
            picked_up_at = NOW() WHERE id = ${deliveryId}::uuid`;
        _ = check db->execute(q);
    } else if status == "DELIVERED" {
        sql:ParameterizedQuery q = `UPDATE deliveries SET status = ${status},
            delivered_at = NOW() WHERE id = ${deliveryId}::uuid`;
        _ = check db->execute(q);
        // free the driver
        sql:ParameterizedQuery q2 = `UPDATE drivers SET is_available = true
            WHERE id = (SELECT driver_id FROM deliveries
                        WHERE id = ${deliveryId}::uuid)`;
        _ = check db->execute(q2);
    } else {
        sql:ParameterizedQuery q = `UPDATE deliveries SET status = ${status}
            WHERE id = ${deliveryId}::uuid`;
        _ = check db->execute(q);
    }
    sql:ParameterizedQuery qb = `INSERT INTO outbox_events
        (event_id, aggregate_type, aggregate_id, topic, payload,
         correlation_id, schema_version)
        VALUES (${env.eventId}::uuid, ${env.aggregateType}, ${env.aggregateId},
                ${env.topic}, ${env.toJsonString()}::jsonb,
                ${env.correlationId}::uuid, ${env.schemaVersion})`;
    _ = check db->execute(qb);
}

// ----- Outbox relay -----
public function fetchUnpublished(int maxRows) returns OutboxRow[]|error {
    sql:ParameterizedQuery q = `SELECT id, event_id, topic, payload::text,
        correlation_id, schema_version
        FROM outbox_events WHERE published_at IS NULL
        ORDER BY id ASC LIMIT ${maxRows} FOR UPDATE SKIP LOCKED`;
    stream<record {
        int id; string event_id; string topic; string payload;
        string correlation_id; int schema_version;
    }, sql:Error?> rs = db->query(q);
    OutboxRow[] out = [];
    check from record {
        int id; string event_id; string topic; string payload;
        string correlation_id; int schema_version;
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

public function isEventProcessed(string eventId, string group) returns boolean|error {
    sql:ParameterizedQuery q = `SELECT 1 AS v FROM processed_events
        WHERE event_id = ${eventId}::uuid AND consumer_group = ${group}`;
    record {|int v;|}? r = check db->queryRow(q);
    return r is record {|int v;|};
}

public function recordProcessed(string eventId, string topic, string group) returns sql:Error? {
    sql:ParameterizedQuery q = `INSERT INTO processed_events (event_id, topic, consumer_group)
        VALUES (${eventId}::uuid, ${topic}, ${group})
        ON CONFLICT (event_id) DO NOTHING`;
    _ = check db->execute(q);
}

public function healthCheck() returns boolean|error {
    sql:ParameterizedQuery q = `SELECT 1 AS v`;
    record {|int v;|}|error r = db->queryRow(q);
    return r is record {|int v;|};
}