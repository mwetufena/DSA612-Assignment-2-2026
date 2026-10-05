// Types for the Restaurant service (same module — no import needed).

public type Restaurant record {|
    string id;
    string name;
    string? cuisine;
    string? address;
    string opensAt;
    string closesAt;
    boolean isOpen;
    string createdAt;
|};

public type MenuItem record {|
    string id;
    string restaurantId;
    string name;
    string? description;
    int priceCents;
    boolean available;
    int stockQty;
    int reservedQty;
|};

public type CreateRestaurantRequest record {|
    string name;
    string? cuisine?;
    string? address?;
    string opensAt?;
    string closesAt?;
|};

public type CreateMenuItemRequest record {|
    string name;
    string? description?;
    int priceCents;
    int initialStock;
|};

public type ReservationRequest record {|
    string orderId;
    string menuItemId;
    int qty;
|};

public type ApiError record {| string code; string message; |};

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

public type ProcessedEvent record {|
    string eventId;
    string topic;
    string consumerGroup;
|};