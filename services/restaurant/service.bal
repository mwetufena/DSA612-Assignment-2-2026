// Restaurant service: REST endpoints + transactional outbox.

import ballerina/http;
import ballerina/time;
import ballerina/uuid;

import ballerinax/kafka;

configurable string KAFKA_BOOTSTRAP = "localhost:9092";
configurable int SERVICE_PORT = 8090;

final kafka:Producer restaurantProducer = check new (KAFKA_BOOTSTRAP);

service / on new http:Listener(SERVICE_PORT) {

    resource function get healthz() returns http:Ok { return http:OK; }

    resource function get readyz() returns http:Ok|http:ServiceUnavailable {
        boolean|error ok = healthCheck();
        if ok is boolean && ok { return http:OK; }
        return http:SERVICE_UNAVAILABLE;
    }

    resource function get restaurants(int maxRows = 50) returns Restaurant[]|error {
        return check listRestaurants(maxRows);
    }

    resource function post restaurants(CreateRestaurantRequest req)
            returns http:Created|ApiError|error {
        if req.name.trim().length() == 0 {
            return <ApiError>{code: "INVALID_INPUT", message: "name required"};
        }
        string id = uuid:createType4AsString();
        string nowIso = time:utcToString(time:utcNow());
        string opensAt = req?.opensAt ?: "08:00:00";
        string closesAt = req?.closesAt ?: "22:00:00";

        Restaurant r = {id, name: req.name, cuisine: req?.cuisine,
            address: req?.address, opensAt, closesAt, isOpen: true, createdAt: nowIso};
        check createRestaurant(r);

        // Emit restaurant.created event
        string corrId = uuid:createType4AsString();
        string evtId = uuid:createType4AsString();
        EventEnvelope env = {
            eventId: evtId,
            correlationId: corrId,
            schemaVersion: 1,
            eventType: "restaurant.created",
            aggregateType: "Restaurant",
            aggregateId: id,
            topic: "restaurant.events",
            occurredAt: nowIso,
            payload: {restaurantId: id, name: req.name, cuisine: req?.cuisine}
        };
        check insertOutboxEvent(env);

        http:Created resp = http:CREATED;
        resp.body = r;
        return resp;
    }

    resource function get restaurants/[string id]() returns Restaurant|ApiError|error {
        return check findRestaurant(id);
    }

    resource function post restaurants/[string id]/menu(CreateMenuItemRequest req)
            returns http:Created|ApiError|error {
        Restaurant|ApiError|error r = findRestaurant(id);
        if r is ApiError { return r; }
        if req.priceCents < 0 {
            return <ApiError>{code: "INVALID_INPUT", message: "priceCents must be >= 0"};
        }
        if req.initialStock < 0 {
            return <ApiError>{code: "INVALID_INPUT", message: "initialStock must be >= 0"};
        }
        string menuItemId = uuid:createType4AsString();
        string nowIso = time:utcToString(time:utcNow());

        MenuItem m = {id: menuItemId, restaurantId: id, name: req.name,
            description: req?.description, priceCents: req.priceCents,
            available: true, stockQty: req.initialStock, reservedQty: 0};
        check createMenuItem(m);

        string corrId = uuid:createType4AsString();
        string evtId = uuid:createType4AsString();
        EventEnvelope env = {
            eventId: evtId,
            correlationId: corrId,
            schemaVersion: 1,
            eventType: "menu.item.added",
            aggregateType: "Restaurant",
            aggregateId: id,
            topic: "restaurant.events",
            occurredAt: nowIso,
            payload: {menuItemId, restaurantId: id, name: req.name,
                priceCents: req.priceCents, stock: req.initialStock}
        };
        check insertOutboxEvent(env);

        http:Created resp = http:CREATED;
        resp.body = m;
        return resp;
    }

    resource function get restaurants/[string id]/menu() returns MenuItem[]|ApiError|error {
        Restaurant|ApiError|error r = findRestaurant(id);
        if r is ApiError { return r; }
        return check listMenu(id);
    }

    // Internal endpoints used by the Order saga
    resource function post inventory/reserve(ReservationRequest req)
            returns http:Ok|ApiError|error {
        boolean|ApiError|error ok = reserveStock(req.orderId, req.menuItemId, req.qty, 600);
        if ok is error { return ok; }
        if ok is ApiError { return ok; }
        return http:OK;
    }

    resource function post inventory/release/[string orderId]() returns http:Ok|error {
        check releaseReservation(orderId);
        return http:OK;
    }

    resource function post inventory/confirm/[string orderId]() returns http:Ok|error {
        check confirmReservation(orderId);
        return http:OK;
    }
}