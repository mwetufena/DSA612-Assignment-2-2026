// Order service: state machine + saga orchestration + outbox.

import ballerina/crypto;
import ballerina/http;
import ballerina/log;
import ballerina/sql;
import ballerina/time;
import ballerina/uuid;

import ballerinax/kafka;

configurable string KAFKA_BOOTSTRAP = "localhost:9092";
configurable int SERVICE_PORT = 8090;
configurable string RESTAURANT_URL = "http://restaurant:8090";
configurable string PAYMENT_URL = "http://payment:8090";
configurable string DELIVERY_URL = "http://delivery:8090";

// HTTP clients for cross-service calls (with timeouts).
final http:Client restaurantClient = check new (RESTAURANT_URL, {timeout: 5});
final http:Client paymentClient = check new (PAYMENT_URL, {timeout: 5});
final http:Client deliveryClient = check new (DELIVERY_URL, {timeout: 5});

final kafka:Producer orderProducer = check new (KAFKA_BOOTSTRAP);

public type ReservationRequest record {|
    string orderId;
    string menuItemId;
    int qty;
|};

public type AssignDriverRequest record {|
    string orderId;
    string pickupAddress;
    string dropoffAddress;
|};

service / on new http:Listener(SERVICE_PORT) {

    resource function get healthz() returns http:Ok { return http:OK; }

    resource function get readyz() returns http:Ok|http:ServiceUnavailable {
        boolean|error ok = healthCheck();
        if ok is boolean && ok { return http:OK; }
        return http:SERVICE_UNAVAILABLE;
    }

    resource function post orders(@http:Header string? idempotencyKey,
                                  CreateOrderRequest req)
            returns http:Created|ApiError|error {
        if req.items.length() == 0 {
            return <ApiError>{code: "INVALID_INPUT", message: "items required"};
        }
        string bodyJson = req.toJsonString();
        string requestHash = crypto:hashMd5(bodyJson.toBytes()).toBase16();

        if idempotencyKey is string && idempotencyKey.length() > 0 {
            ApiError|CachedResponse|error cached = getIdempotentResponse(idempotencyKey, requestHash);
            if cached is ApiError { return cached; }
            if cached is CachedResponse {
                return <http:Created>{
                    body: cached.body,
                    headers: { "Idempotent-Replay": "true" }
                };
            }
        }

        string orderId = uuid:createType4AsString();
        string corrId = uuid:createType4AsString();
        string evtId = uuid:createType4AsString();
        string nowIso = time:utcToString(time:utcNow());

        int totalCents = 0;
        OrderItem[] items = [];
        foreach CreateOrderItemRequest it in req.items {
            int unitPrice = 1000;
            totalCents += unitPrice * it.qty;
            items.push({
                id: uuid:createType4AsString(),
                orderId,
                menuItemId: it.menuItemId,
                name: "Item " + it.menuItemId.substring(0, 8),
                qty: it.qty,
                unitPriceCents: unitPrice
            });
        }

        Order ord = {
            id: orderId,
            customerId: req.customerId,
            restaurantId: req.restaurantId,
            deliveryAddressId: req.deliveryAddressId,
            totalCents,
            state: "CREATED",
            sagaState: "STARTED",
            correlationId: corrId,
            idempotencyKey,
            version: 0,
            createdAt: nowIso,
            updatedAt: nowIso
        };

        EventEnvelope env = {
            eventId: evtId,
            correlationId: corrId,
            schemaVersion: 1,
            eventType: "order.created",
            aggregateType: "Order",
            aggregateId: orderId,
            topic: "order.events",
            occurredAt: nowIso,
            payload: {orderId, customerId: req.customerId,
                restaurantId: req.restaurantId, totalCents, itemsCount: req.items.length()}
        };
        check createOrderWithItems(ord, items, env);

        log:printInfo("order created", correlationId = corrId,
            orderId = orderId, totalCents = totalCents);

        if idempotencyKey is string && idempotencyKey.length() > 0 {
            check storeIdempotentResponse(idempotencyKey, requestHash, 201,
                {"order": ord, "items": items}.toJson());
        }

        http:Created resp = http:CREATED;
        resp.body = {data: ord, items};
        return resp;
    }

    resource function get orders/[string id]() returns OrderView|ApiError|error {
        Order|ApiError|error fetched = findOrder(id);
        if fetched is ApiError { return fetched; }
        if fetched is error { return fetched; }
        Order ord = fetched;
        OrderItem[]|error items = findOrderItems(id);
        if items is error { return items; }
        return {data: ord, items};
    }

    resource function post orders/[string id]/confirm()
            returns Order|ApiError|error {
        Order|ApiError|error fetched = findOrder(id);
        if fetched is ApiError { return fetched; }
        if fetched is error { return fetched; }
        Order ord = fetched;
        if ord.state != "CREATED" {
            return <ApiError>{code: "ILLEGAL_STATE",
                message: "Order must be in CREATED state to confirm"};
        }
        OrderItem[]|error itemsR = findOrderItems(id);
        if itemsR is error { return itemsR; }
        OrderItem[] items = itemsR;

        foreach OrderItem item in items {
            ReservationRequest rq = {orderId: id, menuItemId: item.menuItemId, qty: item.qty};
            http:Response|error resp = restaurantClient->post("/inventory/reserve", rq);
            if resp is error {
                log:printError(string `inventory reserve failed for order=${id}`, resp);
                http:Response|error _r = restaurantClient->post(
                    string `/inventory/release/${id}`, ());
                return <ApiError>{code: "RESERVE_FAILED",
                    message: "Could not reserve inventory"};
            }
            if resp.statusCode != 200 {
                http:Response|error _r2 = restaurantClient->post(
                    string `/inventory/release/${id}`, ());
                return <ApiError>{code: "OUT_OF_STOCK",
                    message: "Restaurant out of stock"};
            }
        }

        EventEnvelope env = buildEvent(ord.correlationId, "order.confirmed", id,
            {orderId: id, totalCents: ord.totalCents},
            time:utcToString(time:utcNow()));
        sql:Error? trErr = transitionState(id, "CREATED", "CONFIRMED", env);
        if trErr is sql:Error {
            http:Response|error _r3 = restaurantClient->post(
                string `/inventory/release/${id}`, ());
            return <ApiError>{code: "DB_ERROR", message: trErr.message()};
        }

        _ = check createPaymentAsync(id, ord.totalCents, ord.customerId, ord.correlationId);
        return check findOrder(id);
    }

    resource function post orders/[string id]/startPreparing()
            returns Order|ApiError|error {
        Order|ApiError|error fetched = findOrder(id);
        if fetched is ApiError { return fetched; }
        if fetched is error { return fetched; }
        Order ord = fetched;
        if ord.state != "CONFIRMED" {
            return <ApiError>{code: "ILLEGAL_STATE", message: "Must be CONFIRMED"};
        }
        EventEnvelope env = buildEvent(ord.correlationId, "order.preparing", id,
            {orderId: id}, time:utcToString(time:utcNow()));
        check transitionState(id, "CONFIRMED", "PREPARING", env);
        return check findOrder(id);
    }

    resource function post orders/[string id]/ready()
            returns Order|ApiError|error {
        Order|ApiError|error fetched = findOrder(id);
        if fetched is ApiError { return fetched; }
        if fetched is error { return fetched; }
        Order ord = fetched;
        if ord.state != "PREPARING" {
            return <ApiError>{code: "ILLEGAL_STATE", message: "Must be PREPARING"};
        }
        EventEnvelope env = buildEvent(ord.correlationId, "order.ready", id,
            {orderId: id}, time:utcToString(time:utcNow()));
        check transitionState(id, "PREPARING", "READY", env);
        return check findOrder(id);
    }

    resource function post orders/[string id]/dispatch()
            returns Order|ApiError|error {
        Order|ApiError|error fetched = findOrder(id);
        if fetched is ApiError { return fetched; }
        if fetched is error { return fetched; }
        Order ord = fetched;
        if ord.state != "READY" {
            return <ApiError>{code: "ILLEGAL_STATE", message: "Must be READY"};
        }
        AssignDriverRequest ar = {orderId: id,
            pickupAddress: "Restaurant " + ord.restaurantId,
            dropoffAddress: "Customer " + ord.deliveryAddressId};
        http:Response|error resp = deliveryClient->post("/deliveries/assign", ar);
        if resp is error {
            return <ApiError>{code: "DISPATCH_FAILED",
                message: "Delivery assignment failed"};
        }
        EventEnvelope env = buildEvent(ord.correlationId, "order.dispatched", id,
            {orderId: id}, time:utcToString(time:utcNow()));
        check transitionState(id, "READY", "OUT_FOR_DELIVERY", env);
        return check findOrder(id);
    }

    resource function post orders/[string id]/deliver()
            returns Order|ApiError|error {
        Order|ApiError|error fetched = findOrder(id);
        if fetched is ApiError { return fetched; }
        if fetched is error { return fetched; }
        Order ord = fetched;
        if ord.state != "OUT_FOR_DELIVERY" {
            return <ApiError>{code: "ILLEGAL_STATE",
                message: "Must be OUT_FOR_DELIVERY"};
        }
        http:Response|error _r = restaurantClient->post(
            string `/inventory/confirm/${id}`, ());
        EventEnvelope env = buildEvent(ord.correlationId, "order.delivered", id,
            {orderId: id, totalCents: ord.totalCents},
            time:utcToString(time:utcNow()));
        check transitionState(id, "OUT_FOR_DELIVERY", "DELIVERED", env);
        return check findOrder(id);
    }

    resource function post orders/[string id]/cancel()
            returns Order|ApiError|error {
        Order|ApiError|error fetched = findOrder(id);
        if fetched is ApiError { return fetched; }
        if fetched is error { return fetched; }
        Order ord = fetched;
        if ord.state != "CREATED" && ord.state != "CONFIRMED" {
            return <ApiError>{code: "ILLEGAL_STATE",
                message: "Cannot cancel from " + ord.state};
        }
        OrderState currentState = ord.state;
        http:Response|error _r = restaurantClient->post(
            string `/inventory/release/${id}`, ());
        EventEnvelope env = buildEvent(ord.correlationId, "order.cancelled", id,
            {orderId: id, reason: "customer-request"},
            time:utcToString(time:utcNow()));
        check transitionState(id, currentState, "CANCELLED", env);
        return check findOrder(id);
    }
}

function buildEvent(string correlationId, string eventType, string orderId,
                    json payload, string nowIso) returns EventEnvelope {
    return {
        eventId: uuid:createType4AsString(),
        correlationId,
        schemaVersion: 1,
        eventType,
        aggregateType: "Order",
        aggregateId: orderId,
        topic: "order.events",
        occurredAt: nowIso,
        payload
    };
}

function createPaymentAsync(string orderId, int amountCents, string customerId,
                            string correlationId) returns error? {
    json body = {orderId, customerId, amountCents, method: "CARD"};
    http:Response|error resp = paymentClient->post("/payments", body);
    if resp is error {
        log:printError(string `payment request failed for order=${orderId}`, resp);
    }
}