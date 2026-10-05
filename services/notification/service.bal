import ballerina/http;
import ballerina/time;
import ballerina/uuid;

import ballerinax/kafka;

configurable string KAFKA_BOOTSTRAP = "localhost:9092";

final kafka:Producer notificationProducer = check new (KAFKA_BOOTSTRAP);

service / on new http:Listener(8090) {

    resource function get healthz() returns http:Ok { return http:OK; }

    resource function get readyz() returns http:Ok|http:ServiceUnavailable {
        boolean|error ok = healthCheck();
        if ok is boolean && ok { return http:OK; }
        return http:SERVICE_UNAVAILABLE;
    }

    resource function post notifications(CreateNotificationRequest req)
            returns http:Created|ApiError|error {
        if req.recipientId.trim().length() == 0 {
            return <ApiError>{code: "INVALID_INPUT", message: "recipientId required"};
        }
        if req.body.trim().length() == 0 {
            return <ApiError>{code: "INVALID_INPUT", message: "body required"};
        }
        string id = uuid:createType4AsString();
        string nowIso = time:utcToString(time:utcNow());
        Notification n = {id, recipientId: req.recipientId,
            channel: req.channel, subject: req?.subject, body: req.body,
            status: "PENDING", relatedEventId: (),
            correlationId: (), createdAt: nowIso, sentAt: ()};
        EventEnvelope env = {
            eventId: uuid:createType4AsString(),
            correlationId: uuid:createType4AsString(),
            schemaVersion: 1,
            eventType: "notification.created",
            aggregateType: "Notification",
            aggregateId: id,
            topic: "notification.events",
            occurredAt: nowIso,
            payload: {notificationId: id, recipientId: req.recipientId,
                channel: req.channel, body: req.body}
        };
        check insertNotification(n, env);
        // Simulate immediate delivery
        check markSent(id);
        http:Created resp = http:CREATED;
        resp.body = n;
        return resp;
    }

    resource function get notifications() returns Notification[]|error {
        return check listAll(100);
    }

    resource function get notifications/recipient/[string id]()
            returns Notification[]|error {
        return check listForRecipient(id, 100);
    }
}