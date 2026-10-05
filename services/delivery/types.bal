public type Driver record {|
    string id;
    string fullName;
    string? phone;
    string? vehicle;
    boolean isAvailable;
    string createdAt;
|};

public type Delivery record {|
    string id;
    string orderId;
    string? driverId;
    string pickupAddress;
    string dropoffAddress;
    string status;
    string createdAt;
    string? assignedAt;
    string? pickedUpAt;
    string? deliveredAt;
|};

public type CreateDriverRequest record {|
    string fullName;
    string? phone?;
    string? vehicle?;
|};

public type AssignRequest record {|
    string orderId;
    string pickupAddress;
    string dropoffAddress;
|};

public type StatusUpdateRequest record {|
    string status;
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