import ballerina/sql;
import ballerinax/postgresql;
import ballerinax/postgresql.driver as _;

configurable string DB_HOST = "localhost";
configurable string DB_NAME = "notification_db";
configurable string DB_USER = "postgres";
configurable string DB_PASSWORD = "postgres";

final postgresql:Client db = check new (
    host = DB_HOST, database = DB_NAME, username = DB_USER, password = DB_PASSWORD);

public function insertNotification(Notification n, EventEnvelope env) returns sql:Error? {
    sql:ParameterizedQuery q = `INSERT INTO notifications
        (id, recipient_id, channel, subject, body, status,
         related_event_id, correlation_id)
        VALUES (${n.id}::uuid, ${n.recipientId}::uuid, ${n.channel},
                ${n?.subject}, ${n.body}, ${n.status},
                ${env.eventId}::uuid, ${env.correlationId}::uuid)`;
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

public function markSent(string id) returns sql:Error? {
    sql:ParameterizedQuery q = `UPDATE notifications SET status = 'SENT',
        sent_at = NOW() WHERE id = ${id}::uuid`;
    _ = check db->execute(q);
}

public function listForRecipient(string recipientId, int maxRows)
    returns Notification[]|error {
    sql:ParameterizedQuery q = `SELECT id, recipient_id::text, channel, subject,
        body, status, related_event_id::text, correlation_id::text,
        created_at::text, sent_at::text
        FROM notifications WHERE recipient_id = ${recipientId}::uuid
        ORDER BY created_at DESC LIMIT ${maxRows}`;
    stream<record {
        string id; string recipient_id; string channel; string? subject;
        string body; string status; string? related_event_id;
        string? correlation_id; string created_at; string? sent_at;
    }, sql:Error?> rs = db->query(q);
    Notification[] out = [];
    check from record {
        string id; string recipient_id; string channel; string? subject;
        string body; string status; string? related_event_id;
        string? correlation_id; string created_at; string? sent_at;
    } r in rs
        do {
            out.push({id: r.id, recipientId: r.recipient_id,
                channel: <Channel>r.channel, subject: r.subject,
                body: r.body, status: r.status,
                relatedEventId: r.related_event_id,
                correlationId: r.correlation_id,
                createdAt: r.created_at, sentAt: r.sent_at});
        };
    return out;
}

public function listAll(int maxRows) returns Notification[]|error {
    sql:ParameterizedQuery q = `SELECT id, recipient_id::text, channel, subject,
        body, status, related_event_id::text, correlation_id::text,
        created_at::text, sent_at::text
        FROM notifications ORDER BY created_at DESC LIMIT ${maxRows}`;
    stream<record {
        string id; string recipient_id; string channel; string? subject;
        string body; string status; string? related_event_id;
        string? correlation_id; string created_at; string? sent_at;
    }, sql:Error?> rs = db->query(q);
    Notification[] out = [];
    check from record {
        string id; string recipient_id; string channel; string? subject;
        string body; string status; string? related_event_id;
        string? correlation_id; string created_at; string? sent_at;
    } r in rs
        do {
            out.push({id: r.id, recipientId: r.recipient_id,
                channel: <Channel>r.channel, subject: r.subject,
                body: r.body, status: r.status,
                relatedEventId: r.related_event_id,
                correlationId: r.correlation_id,
                createdAt: r.created_at, sentAt: r.sent_at});
        };
    return out;
}

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