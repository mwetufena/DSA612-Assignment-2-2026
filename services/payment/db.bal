// PostgreSQL helpers for the Payment service.

import ballerina/sql;
import ballerinax/postgresql;
import ballerinax/postgresql.driver as _;

configurable string DB_HOST = "localhost";
configurable int DB_PORT = 5432;
configurable string DB_NAME = "payment_db";
configurable string DB_USER = "postgres";
configurable string DB_PASSWORD = "postgres";

final postgresql:Client db = check initClient();

function initClient() returns postgresql:Client|error => new (
    host = DB_HOST, port = DB_PORT, database = DB_NAME,
    username = DB_USER, password = DB_PASSWORD
);

// Create payment + outbox event in one transaction
public function createPaymentWithEvent(Payment p, EventEnvelope env) returns sql:Error? {
    sql:ParameterizedQuery ins = `INSERT INTO payments
        (id, order_id, customer_id, amount_cents, method, status)
        VALUES (${p.id}::uuid, ${p.orderId}::uuid, ${p.customerId}::uuid,
                ${p.amountCents}, ${p.method}, ${p.status})`;
    sql:ExecutionResult|sql:Error r1 = db->execute(ins);
    if r1 is sql:Error { return r1; }

    sql:ParameterizedQuery insBox = `INSERT INTO outbox_events
        (event_id, aggregate_type, aggregate_id, topic, payload,
         correlation_id, schema_version)
        VALUES (${env.eventId}::uuid, ${env.aggregateType}, ${env.aggregateId},
                ${env.topic}, ${env.toJsonString()}::jsonb,
                ${env.correlationId}::uuid, ${env.schemaVersion})`;
    sql:ExecutionResult|sql:Error r2 = db->execute(insBox);
    if r2 is sql:Error { return r2; }
}

public function completePayment(string paymentId, string newStatus,
                                 string? transactionRef, string? failureReason)
    returns sql:Error? {
    sql:ParameterizedQuery q = `UPDATE payments
        SET status = ${newStatus}, transaction_ref = ${transactionRef},
            failure_reason = ${failureReason}, completed_at = NOW()
        WHERE id = ${paymentId}::uuid`;
    _ = check db->execute(q);
}

public function findPayment(string id) returns Payment|ApiError|error {
    sql:ParameterizedQuery q = `SELECT id, order_id::text, customer_id::text,
        amount_cents, method, status, transaction_ref, failure_reason,
        created_at::text, completed_at::text
        FROM payments WHERE id = ${id}::uuid`;
    record {
        string id; string order_id; string customer_id; int amount_cents;
        string method; string status; string? transaction_ref;
        string? failure_reason; string created_at; string? completed_at;
    }? r = check db->queryRow(q);
    if r is () { return <ApiError>{code: "NOT_FOUND", message: "Payment not found"}; }
    return {id: r.id, orderId: r.order_id, customerId: r.customer_id,
        amountCents: r.amount_cents, method: <PaymentMethod>r.method,
        status: r.status, transactionRef: r.transaction_ref,
        failureReason: r.failure_reason, createdAt: r.created_at,
        completedAt: r.completed_at};
}

public function findPaymentByOrder(string orderId) returns Payment?|error {
    sql:ParameterizedQuery q = `SELECT id, order_id::text, customer_id::text,
        amount_cents, method, status, transaction_ref, failure_reason,
        created_at::text, completed_at::text
        FROM payments WHERE order_id = ${orderId}::uuid
        ORDER BY created_at DESC LIMIT 1`;
    record {
        string id; string order_id; string customer_id; int amount_cents;
        string method; string status; string? transaction_ref;
        string? failure_reason; string created_at; string? completed_at;
    }? r = check db->queryRow(q);
    if r is () { return (); }
    return {id: r.id, orderId: r.order_id, customerId: r.customer_id,
        amountCents: r.amount_cents, method: <PaymentMethod>r.method,
        status: r.status, transactionRef: r.transaction_ref,
        failureReason: r.failure_reason, createdAt: r.created_at,
        completedAt: r.completed_at};
}

public function listPayments(int maxRows) returns Payment[]|error {
    sql:ParameterizedQuery q = `SELECT id, order_id::text, customer_id::text,
        amount_cents, method, status, transaction_ref, failure_reason,
        created_at::text, completed_at::text
        FROM payments ORDER BY created_at DESC LIMIT ${maxRows}`;
    stream<record {
        string id; string order_id; string customer_id; int amount_cents;
        string method; string status; string? transaction_ref;
        string? failure_reason; string created_at; string? completed_at;
    }, sql:Error?> rs = db->query(q);
    Payment[] out = [];
    check from record {
        string id; string order_id; string customer_id; int amount_cents;
        string method; string status; string? transaction_ref;
        string? failure_reason; string created_at; string? completed_at;
    } r in rs
        do {
            out.push({id: r.id, orderId: r.order_id, customerId: r.customer_id,
                amountCents: r.amount_cents, method: <PaymentMethod>r.method,
                status: r.status, transactionRef: r.transaction_ref,
                failureReason: r.failure_reason, createdAt: r.created_at,
                completedAt: r.completed_at});
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

// ----- Consumer idempotency -----
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