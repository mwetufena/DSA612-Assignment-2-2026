// Outbox relay for the Restaurant service.

import ballerina/lang.runtime;
import ballerina/log;
import ballerina/sql;
import ballerina/task;

import ballerinax/kafka;

configurable decimal RELAY_INTERVAL_SECONDS = 1;
configurable int RELAY_BATCH = 50;

class OutboxRelayJob {
    *task:Job;

    public function execute() {
        OutboxRow[]|error rows = fetchUnpublished(RELAY_BATCH);
        if rows is error {
            log:printError("outbox fetch failed", rows);
            return;
        }
        if rows.length() == 0 { return; }

        log:printInfo("outbox relay publishing", count = rows.length());
        foreach OutboxRow row in rows {
            json|error envelope = row.payload.fromJsonString();
            if envelope is error {
                log:printError(string `payload not JSON id=${row.id}`, envelope);
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
            error? sent = restaurantProducer->send(r);
            if sent is error {
                log:printError(string `kafka send failed eventId=${row.eventId}`, sent);
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
    _ = check task:scheduleJobRecurByFrequency(new OutboxRelayJob(), RELAY_INTERVAL_SECONDS);
    while true {
        log:printDebug("restaurant service alive");
        runtime:sleep(60);
    }
}