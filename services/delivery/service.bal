import ballerina/http;
import ballerina/sql;
import ballerina/time;
import ballerina/uuid;

import ballerinax/kafka;

configurable string KAFKA_BOOTSTRAP = "localhost:9092";

final kafka:Producer deliveryProducer = check new (KAFKA_BOOTSTRAP);

service / on new http:Listener(8090) {

    resource function get healthz() returns http:Ok { return http:OK; }

    resource function get readyz() returns http:Ok|http:ServiceUnavailable {
        boolean|error ok = healthCheck();
        if ok is boolean && ok { return http:OK; }
        return http:SERVICE_UNAVAILABLE;
    }

    resource function get drivers() returns Driver[]|error {
        return check listDrivers();
    }

    resource function post drivers(CreateDriverRequest req)
            returns http:Created|ApiError|error {
        if req.fullName.trim().length() == 0 {
            return <ApiError>{code: "INVALID_INPUT", message: "fullName required"};
        }
        string id = uuid:createType4AsString();
        Driver d = {id, fullName: req.fullName, phone: req?.phone,
            vehicle: req?.vehicle, isAvailable: true,
            createdAt: time:utcToString(time:utcNow())};
        check createDriver(d);
        http:Created resp = http:CREATED;
        resp.body = d;
        return resp;
    }

    resource function post deliveries/assign(AssignRequest req)
            returns http:Created|ApiError|error {
        string|error driverId = findAvailableDriver();
        if driverId is error {
            return <ApiError>{code: "NO_DRIVER",
                message: "No drivers available"};
        }
        string driverIdStr = driverId;
        check markDriverBusy(driverIdStr);

        string deliveryId = uuid:createType4AsString();
        string nowIso = time:utcToString(time:utcNow());
        string corrId = uuid:createType4AsString();
        string evtId = uuid:createType4AsString();

        Delivery d = {id: deliveryId, orderId: req.orderId,
            driverId: driverIdStr, pickupAddress: req.pickupAddress,
            dropoffAddress: req.dropoffAddress, status: "ASSIGNED",
            createdAt: nowIso, assignedAt: nowIso,
            pickedUpAt: (), deliveredAt: ()};

        EventEnvelope env = {
            eventId: evtId,
            correlationId: corrId,
            schemaVersion: 1,
            eventType: "delivery.assigned",
            aggregateType: "Delivery",
            aggregateId: deliveryId,
            topic: "delivery.events",
            occurredAt: nowIso,
            payload: {deliveryId, orderId: req.orderId, driverId: driverIdStr}
        };
        check createDeliveryWithEvent(d, env);

        http:Created resp = http:CREATED;
        resp.body = d;
        return resp;
    }

    resource function get deliveries/[string id]() returns Delivery|ApiError|error {
        return check findDelivery(id);
    }

    resource function get deliveries/findByOrder/[string orderId]()
            returns Delivery|ApiError|error {
        Delivery?|error r = findDeliveryByOrder(orderId);
        if r is error { return r; }
        if r is () { return <ApiError>{code: "NOT_FOUND", message: "No delivery for order"}; }
        return r;
    }

    resource function post deliveries/[string id]/status(StatusUpdateRequest req)
            returns Delivery|ApiError|error {
        Delivery|ApiError|error fetched = findDelivery(id);
        if fetched is ApiError { return fetched; }
        if fetched is error { return fetched; }
        Delivery d = fetched;

        if req.status != "PICKED_UP" && req.status != "DELIVERED" {
            return <ApiError>{code: "INVALID_INPUT",
                message: "status must be PICKED_UP or DELIVERED"};
        }

        EventEnvelope env = {
            eventId: uuid:createType4AsString(),
            correlationId: uuid:createType4AsString(),
            schemaVersion: 1,
            eventType: "delivery." + req.status.toLowerAscii(),
            aggregateType: "Delivery",
            aggregateId: id,
            topic: "delivery.events",
            occurredAt: time:utcToString(time:utcNow()),
            payload: {deliveryId: id, orderId: d.orderId, status: req.status}
        };
        sql:Error? upd = updateDeliveryStatus(id, req.status, env);
        if upd is sql:Error {
            return <ApiError>{code: "DB_ERROR", message: upd.message()};
        }
        return check findDelivery(id);
    }
}