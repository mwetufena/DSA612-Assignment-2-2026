// Outbox relay for the Payment service.

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
        if rows is error { return; }
        if rows.length() == 0 { return; }
        log:printInfo("payment outbox publishing", count = rows.length());
        foreach OutboxRow row in rows {
            json|error envelope = row.payload.fromJsonString();
            if envelope is error { continue; }
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
            error? sent = paymentProducer->send(r);
            if sent is error { continue; }
            sql:Error? marked = markPublished(row.id);
        }
    }
}

public function main() returns error? {
    _ = check task:scheduleJobRecurByFrequency(new OutboxRelayJob(), RELAY_INTERVAL_SECONDS);
    while true { runtime:sleep(60); }
}
