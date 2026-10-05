// Customer service: REST API + idempotency + transactional outbox.

import ballerina/crypto;
import ballerina/http;
import ballerina/log;
import ballerina/time;
import ballerina/uuid;

import ballerinax/kafka;

configurable string KAFKA_BOOTSTRAP = "localhost:9092";
configurable int SERVICE_PORT = 8090;

final kafka:Producer customerProducer = check new (KAFKA_BOOTSTRAP);

service / on new http:Listener(SERVICE_PORT) {

    resource function get healthz() returns http:Ok {
        return http:OK;
    }

    resource function get readyz() returns http:Ok|http:ServiceUnavailable {
        boolean|error ok = healthCheck();
        if ok is boolean && ok {
            return http:OK;
        }
        return http:SERVICE_UNAVAILABLE;
    }

    resource function get customers(int maxRows = 50) returns Customer[]|ApiError|error {
        return check listCustomers(maxRows);
    }

    resource function post customers(@http:Header string? idempotencyKey,
                                     CreateCustomerRequest req)
            returns http:Created|ApiError|error {

        if req.email.trim().length() == 0 || !req.email.includes("@") {
            return <ApiError>{code: "INVALID_INPUT", message: "Valid email required"};
        }
        if req.fullName.trim().length() == 0 {
            return <ApiError>{code: "INVALID_INPUT", message: "fullName is required"};
        }

        string bodyJson = req.toJsonString();
        string requestHash = crypto:hashMd5(bodyJson.toBytes()).toBase16();

        if idempotencyKey is string && idempotencyKey.length() > 0 {
            ApiError|CachedResponse|error cached = getIdempotentResponse(idempotencyKey, requestHash);
            if cached is ApiError {
                return cached;
            }
            if cached is CachedResponse {
                return <http:Created>{
                    body: cached.body,
                    headers: { "Idempotent-Replay": "true" }
                };
            }
        }

        string custId = uuid:createType4AsString();
        string corrId = uuid:createType4AsString();
        string evtId = uuid:createType4AsString();
        string nowIso = time:utcToString(time:utcNow());

        Customer c = {id: custId, email: req.email, fullName: req.fullName,
            phone: req?.phone, createdAt: nowIso};

        json eventPayload = {customerId: custId, email: req.email,
            fullName: req.fullName, phone: req?.phone, createdAt: nowIso};

        EventEnvelope env = {
            eventId: evtId,
            correlationId: corrId,
            schemaVersion: 1,
            eventType: "customer.created",
            aggregateType: "Customer",
            aggregateId: custId,
            topic: "customer.events",
            occurredAt: nowIso,
            payload: eventPayload
        };
        check createCustomerWithEvent(c, env);

        log:printInfo("customer created", correlationId = corrId,
            customerId = custId, email = req.email);

        http:Created resp = http:CREATED;
        resp.body = c;
        if idempotencyKey is string && idempotencyKey.length() > 0 {
            check storeIdempotentResponse(idempotencyKey, requestHash, 201, c.toJson());
        }
        return resp;
    }

    resource function get customers/[string id]() returns Customer|ApiError|error {
        return check findCustomer(id);
    }

    resource function get customers/[string id]/addresses() returns Address[]|ApiError|error {
        Customer|ApiError|error c = findCustomer(id);
        if c is ApiError { return c; }
        return check findAddresses(id);
    }

    resource function post customers/[string id]/addresses(CreateAddressRequest req)
            returns http:Created|ApiError|error {
        Customer|ApiError|error c = findCustomer(id);
        if c is ApiError { return c; }

        if req.line1.trim().length() == 0 || req.city.trim().length() == 0 {
            return <ApiError>{code: "INVALID_INPUT", message: "line1 and city are required"};
        }
        string addrId = uuid:createType4AsString();
        string corrId = uuid:createType4AsString();
        string evtId = uuid:createType4AsString();
        string nowIso = time:utcToString(time:utcNow());

        boolean isDefault = req?.isDefault == true;
        Address a = {id: addrId, customerId: id, line1: req.line1, city: req.city,
            postalCode: req.postalCode, isDefault, createdAt: nowIso};

        EventEnvelope env = {
            eventId: evtId,
            correlationId: corrId,
            schemaVersion: 1,
            eventType: "customer.address.added",
            aggregateType: "Customer",
            aggregateId: id,
            topic: "customer.events",
            occurredAt: nowIso,
            payload: {addressId: addrId, customerId: id, city: req.city}
        };
        check createAddressWithEvent(id, a, env);

        http:Created resp = http:CREATED;
        resp.body = a;
        return resp;

    }
}
