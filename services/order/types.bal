// Types for the Order service.

public type OrderState "CREATED"|"CONFIRMED"|"PREPARING"|"READY"|"OUT_FOR_DELIVERY"|"DELIVERED"|"CANCELLED"|"REJECTED";

public type Order record {|
    string id;
    string customerId;
    string restaurantId;
    string deliveryAddressId;
    int totalCents;
    OrderState state;
    string sagaState;
    string correlationId;
    string? idempotencyKey;
    int version;
    string createdAt;
    string updatedAt;
|};

public type OrderItem record {|
    string id;
    string orderId;
    string menuItemId;
    string name;
    int qty;
    int unitPriceCents;
|};

public type CreateOrderItemRequest record {|
    string menuItemId;
    int qty;
|};

public type CreateOrderRequest record {|
    string customerId;
    string restaurantId;
    string deliveryAddressId;
    CreateOrderItemRequest[] items;
|};

public type ApiError record {| string code; string message; |};

public type OrderView record {|
    Order data;
    OrderItem[] items;
|};

public type OutboxRow record {|
    int id;
    string eventId;
    string topic;
    string payload;
    string correlationId;
    int schemaVersion;
|};

public type EventEnvelope record {|
    string eventId;
    string correlationId;
    int schemaVersion;
    string eventType;
    string aggregateType;
    string aggregateId;
    string topic;
    string occurredAt;
    json payload;
|};

public type CachedResponse record {| int status; json body; |};