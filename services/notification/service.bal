// Payment service: simulated processor + outbox + circuit breaker.

import ballerina/http;
import ballerina/random;
import ballerina/sql;
import ballerina/time;
import ballerina/uuid;

import ballerinax/kafka;

configurable string KAFKA_BOOTSTRAP = "localhost:9092";
configurable int SERVICE_PORT = 8090;
configurable decimal PAYMENT_FAILURE_RATE = 0.05; // 5% simulated failures

final kafka:Producer paymentProducer = check new (KAFKA_BOOTSTRAP);

service / on new http:Listener(SERVICE_PORT) {

    resource function get healthz() returns http:Ok { return http:OK; }

    resource function get readyz() returns http:Ok|http:ServiceUnavailable {
        boolean|error ok = healthCheck();
        if ok is boolean && ok { return http:OK; }
        return http:SERVICE_UNAVAILABLE;
    }

    resource function post payments(CreatePaymentRequest req)
            returns http:Created|ApiError|error {
        if req.amountCents <= 0 {
            return <ApiError>{code: "INVALID_INPUT",
                message: "amountCents must be > 0"};
        }

        string paymentId = uuid:createType4AsString();
        string corrId = uuid:createType4AsString();
        string evtId = uuid:createType4AsString();
        string nowIso = time:utcToString(time:utcNow());

        Payment p = {id: paymentId, orderId: req.orderId,
            customerId: req.customerId, amountCents: req.amountCents,
            method: req.method, status: "PENDING", transactionRef: (),
            failureReason: (), createdAt: nowIso, completedAt: ()};

        EventEnvelope env = {
            eventId: evtId,
            correlationId: corrId,
            schemaVersion: 1,
            eventType: "payment.requested",
            aggregateType: "Payment",
            aggregateId: paymentId,
            topic: "payment.events",
            occurredAt: nowIso,
            payload: {paymentId, orderId: req.orderId,
                customerId: req.customerId, amountCents: req.amountCents}
        };
        check createPaymentWithEvent(p, env);

        // Simulated processor decision
        int|error rollR = random:createIntInRange(0, 100);
        int roll = rollR is error ? 50 : rollR;
        float failureThreshold = <float>PAYMENT_FAILURE_RATE * 100.0;
        if roll < <int>failureThreshold {
            string transactionRef = "TXN-" + paymentId.substring(0, 8);
            check completePayment(paymentId, "FAILED", transactionRef,
                "Simulated processor decline");
            EventEnvelope failEnv = {
                eventId: uuid:createType4AsString(),
                correlationId: corrId,
                schemaVersion: 1,
                eventType: "payment.failed",
                aggregateType: "Payment",
                aggregateId: paymentId,
                topic: "payment.events",
                occurredAt: time:utcToString(time:utcNow()),
                payload: {paymentId, orderId: req.orderId,
                    failureReason: "Simulated decline"}
            };
            check insertOutboxEvent(failEnv);
        } else {
            string transactionRef = "TXN-" + paymentId.substring(0, 8);
            check completePayment(paymentId, "SUCCESS", transactionRef, ());
            EventEnvelope okEnv = {
                eventId: uuid:createType4AsString(),
                correlationId: corrId,
                schemaVersion: 1,
                eventType: "payment.success",
                aggregateType: "Payment",
                aggregateId: paymentId,
                topic: "payment.events",
                occurredAt: time:utcToString(time:utcNow()),
                payload: {paymentId, orderId: req.orderId,
                    amountCents: req.amountCents, transactionRef}
            };
            check insertOutboxEvent(okEnv);
        }

        http:Created resp = http:CREATED;
        resp.body = check findPayment(paymentId);
        return resp;
    }

    resource function get payments/[string id]() returns Payment|ApiError|error {
        return check findPayment(id);
    }

    resource function get payments() returns Payment[]|error {
        return check listPayments(50);
    }
}

public function insertOutboxEvent(EventEnvelope env) returns sql:Error? {
    sql:ParameterizedQuery q = `INSERT INTO outbox_events
        (event_id, aggregate_type, aggregate_id, topic, payload,
         correlation_id, schema_version)
        VALUES (${env.eventId}::uuid, ${env.aggregateType}, ${env.aggregateId},
                ${env.topic}, ${env.toJsonString()}::jsonb,
                ${env.correlationId}::uuid, ${env.schemaVersion})`;
    _ = check db->execute(q);
}
