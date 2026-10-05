// Admin service Kafka consumer: builds read-model projections (mini-CQRS).

import ballerina/lang.runtime;
import ballerina/log;
import ballerina/sql;
import ballerina/task;

import ballerinax/kafka;

configurable string KAFKA_BOOTSTRAP = "localhost:9092";
configurable string KAFKA_GROUP = "admin-projections";
configurable decimal POLL_TIMEOUT_SECONDS = 5;
configurable string[] KAFKA_TOPICS = ["order.events", "delivery.events", "payment.events"];

final kafka:Consumer adminConsumer = check new (KAFKA_BOOTSTRAP, {
    groupId: KAFKA_GROUP,
    topics: KAFKA_TOPICS,
    offsetReset: "earliest",
    autoCommit: false
});

class ProjectionJob {
    *task:Job;

    public function execute() {
        kafka:AnydataConsumerRecord[]|error pollResult = adminConsumer->poll(POLL_TIMEOUT_SECONDS);
        if pollResult is error {
            log:printError("kafka poll failed", pollResult);
            return;
        }
        foreach kafka:AnydataConsumerRecord rec in pollResult {
            string topic = rec.offset.partition.topic;
            map<json>|error envelopeR = rec.value.cloneWithType();
            if envelopeR is error {
                log:printError("invalid envelope, skipping", envelopeR);
                continue;
            }
            map<json> envelope = envelopeR;

            json|error evIdR = envelope["eventId"];
            json|error evTypeR = envelope["eventType"];
            json|error evAtR = envelope["occurredAt"];
            json|error payloadR = envelope["payload"];

            if evIdR is error || evTypeR is error || evAtR is error || payloadR is error {
                continue;
            }

            string eventId = evIdR is string ? evIdR : evIdR.toString();
            string eventType = evTypeR is string ? evTypeR : evTypeR.toString();
            string occurredAt = evAtR is string ? evAtR : evAtR.toString();
            json payload = payloadR;

            boolean|error seen = isEventProcessed(eventId, KAFKA_GROUP);
            if seen is error { continue; }
            if seen { continue; }

            sql:Error? err = ();
            if topic == "order.events" {
                err = projectOrderEvent(eventType, payload, occurredAt);
            } else if topic == "delivery.events" {
                err = projectDeliveryEvent(eventType, payload, occurredAt);
            }
            if err is sql:Error {
                log:printError(string `projection failed for event ${eventId}`, err);
                continue;
            }
            sql:Error? recorded = recordProcessed(eventId, topic, KAFKA_GROUP);
            if recorded is sql:Error {
                log:printError(string `record processed failed for event ${eventId}`, recorded);
            }
        }
        error? commitErr = adminConsumer->commit();
        if commitErr is error {
            log:printError("commit failed", commitErr);
        }
    }
}

public function main() returns error? {
    _ = check task:scheduleJobRecurByFrequency(new ProjectionJob(), 2d);
    while true { runtime:sleep(60); }
}
