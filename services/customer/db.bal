// PostgreSQL helpers for the Customer service.

import ballerina/sql;
import ballerinax/postgresql;
import ballerinax/postgresql.driver as _;

configurable string DB_HOST = "localhost";
configurable int DB_PORT = 5432;
configurable string DB_NAME = "customer_db";
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
        return <ApiError>{code: "IDEMPOTENCY_CONFLICT", message: "Key reused with different payload"};
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

// ----- Customer -----
public function findCustomer(string id) returns Customer|ApiError|error {
    sql:ParameterizedQuery sq = `SELECT id, email, full_name, phone, created_at::text
        FROM customers WHERE id = ${id}::uuid`;
    record {string id; string email; string full_name; string? phone; string created_at;}? row =
        check db->queryRow(sq);
    if row is () {
        return <ApiError>{code: "NOT_FOUND", message: "Customer not found"};
    }
    return {id: row.id, email: row.email, fullName: row.full_name,
        phone: row.phone, createdAt: row.created_at};
}

public function listCustomers(int maxRows) returns Customer[]|error {
    sql:ParameterizedQuery q = `SELECT id, email, full_name, phone, created_at::text
        FROM customers ORDER BY created_at DESC LIMIT ${maxRows}`;
    stream<record {
        string id; string email; string full_name; string? phone; string created_at;
    }, sql:Error?> rs = db->query(q);

    Customer[] out = [];
    check from record {
        string id; string email; string full_name; string? phone; string created_at;
    } r in rs
        do {
            out.push({id: r.id, email: r.email, fullName: r.full_name,
                phone: r.phone, createdAt: r.created_at});
        };
    return out;
}

public function findAddresses(string customerId) returns Address[]|error {
    sql:ParameterizedQuery q = `SELECT id, customer_id, line1, city, postal_code,
            is_default, created_at::text
        FROM addresses WHERE customer_id = ${customerId}::uuid
        ORDER BY is_default DESC, created_at DESC`;
    stream<record {
        string id; string customer_id; string line1; string city; string postal_code;
        boolean is_default; string created_at;
    }, sql:Error?> rs = db->query(q);

    Address[] out = [];
    check from record {
        string id; string customer_id; string line1; string city; string postal_code;
        boolean is_default; string created_at;
    } r in rs
        do {
            out.push({id: r.id, customerId: r.customer_id, line1: r.line1,
                city: r.city, postalCode: r.postal_code, isDefault: r.is_default,
                createdAt: r.created_at});
        };
    return out;
}

// ----- Outbox writes -----
public function createCustomerWithEvent(Customer customer, EventEnvelope env) returns error? {
    sql:ParameterizedQuery insCust = `INSERT INTO customers
        (id, email, full_name, phone) VALUES (
            ${customer.id}::uuid, ${customer.email},
            ${customer.fullName}, ${customer.phone})`;

    sql:ParameterizedQuery insBox = `INSERT INTO outbox_events
        (event_id, aggregate_type, aggregate_id, topic, payload, correlation_id, schema_version)
        VALUES (${env.eventId}::uuid, ${env.aggregateType}, ${env.aggregateId},
                ${env.topic}, ${env.toJsonString()}::jsonb, ${env.correlationId}::uuid,
                ${env.schemaVersion})`;

    sql:ExecutionResult|sql:Error r1 = db->execute(insCust);
    if r1 is sql:Error { return r1; }
    sql:ExecutionResult|sql:Error r2 = db->execute(insBox);
    if r2 is sql:Error { return r2; }
}

public function createAddressWithEvent(string customerId, Address addr,
                                       EventEnvelope env) returns error? {
    sql:ParameterizedQuery insAddr = `INSERT INTO addresses
        (id, customer_id, line1, city, postal_code, is_default) VALUES (
            ${addr.id}::uuid, ${customerId}::uuid, ${addr.line1},
            ${addr.city}, ${addr.postalCode}, ${addr.isDefault})`;

    sql:ParameterizedQuery insBox = `INSERT INTO outbox_events
        (event_id, aggregate_type, aggregate_id, topic, payload, correlation_id, schema_version)
        VALUES (${env.eventId}::uuid, ${env.aggregateType}, ${env.aggregateId},
                ${env.topic}, ${env.toJsonString()}::jsonb, ${env.correlationId}::uuid,
                ${env.schemaVersion})`;

    sql:ExecutionResult|sql:Error r1 = db->execute(insAddr);
    if r1 is sql:Error { return r1; }
    sql:ExecutionResult|sql:Error r2 = db->execute(insBox);
    if r2 is sql:Error { return r2; }
}

// ----- Outbox relay support -----
public function fetchUnpublished(int maxRows) returns OutboxRow[]|error {
    sql:ParameterizedQuery q = `SELECT id, event_id, topic, payload::text,
            correlation_id, schema_version
        FROM outbox_events
        WHERE published_at IS NULL
        ORDER BY id ASC
        LIMIT ${maxRows}
        FOR UPDATE SKIP LOCKED`;
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

// ----- Health -----
public function healthCheck() returns boolean|error {
    sql:ParameterizedQuery q = `SELECT 1 AS v`;
    record {|int v;|}|error r = db->queryRow(q);
    return r is record {|int v;|};
}