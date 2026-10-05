// Shared types for the Customer service (same module as service.bal etc.).

public type Customer record {|
    string id;
    string email;
    string fullName;
    string? phone;
    string createdAt;
|};

public type Address record {|
    string id;
    string customerId;
    string line1;
    string city;
    string postalCode;
    boolean isDefault;
    string createdAt;
|};

public type CreateCustomerRequest record {|
    string email;
    string fullName;
    string? phone?;
|};

public type CreateAddressRequest record {|
    string line1;
    string city;
    string postalCode;
    boolean isDefault?;
|};

public type ApiError record {|
    string code;
    string message;
|};

// Outbox row used by the relay
public type OutboxRow record {|
    int id;
    string eventId;
    string topic;
    string payload;
    string correlationId;
    int schemaVersion;
|};

// Canonical Kafka event envelope (whole platform).
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

// Stored idempotency response (for cached HTTP replays)
public type CachedResponse record {|
    int status;
    json body;
|};
