// Transactional outbox relay.
// Polls outbox_events, publishes unpublished events to Kafka, marks as published.
// Idempotent and concurrency-safe via FOR UPDATE SKIP LOCKED.

import ballerina/lang.runtime;
import ballerina/log;
import ballerina/sql;
import ballerina/task;

import ballerinax/kafka;

configurable decimal RELAY_INTERVAL_SECONDS = 1;
configurable int RELAY_BATCH = 50;

// Job class: scheduler calls execute() periodically.
class OutboxRelayJob {
    *task:Job;

    public function execute() {
        OutboxRow[]|error rows = fetchUnpublished(RELAY_BATCH);
        if rows is error {
            log:printError("outbox fetch failed", rows);
            return;
        }
        if rows.length() == 0 {
            return;
        }
        log:printInfo("outbox relay publishing", count = rows.length());
        foreach OutboxRow row in rows {
            json|error envelope = row.payload.fromJsonString();
            if envelope is error {
                log:printError(string `payload not JSON; skipping id=${row.id}`,
                    envelope);
                continue;
            }
            map<string> hdrs = {
                "eventId": row.eventId,
                "correlationId": row.correlationId,
                "schemaVersion": row.schemaVersion.toString()
            };
            kafka:AnydataProducerRecord r = {
                topic: row.topic,
                key: row.eventId,
                value: envelope,
                headers: hdrs
            };
            error? sent = customerProducer->send(r);
            if sent is error {
                log:printError(string `kafka send failed; will retry eventId=${row.eventId}`,
                    sent);
                continue;
            }
            sql:Error? marked = markPublished(row.id);
            if marked is sql:Error {
                log:printError(string `failed to mark published id=${row.id}`, marked);
            }
        }
    }
}

public function main() returns error? {
    // Schedule the outbox relay. The HTTP listener starts automatically because
    // service.bal declares a `service /` on a listener at module init.
    _ = check task:scheduleJobRecurByFrequency(new OutboxRelayJob(), RELAY_INTERVAL_SECONDS);

    // Keep the main function alive; HTTP listener and task scheduler run on
    // their own strands.
    while true {
        log:printDebug("customer service alive");
        runtime:sleep(60);
    }
}