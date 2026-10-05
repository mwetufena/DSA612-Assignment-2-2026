// Types for the Payment service.

public type PaymentMethod "CARD"|"WALLET"|"CASH";

public type Payment record {|
    string id;
    string orderId;
    string customerId;
    int amountCents;
    PaymentMethod method;
    string status;
    string? transactionRef;
    string? failureReason;
    string createdAt;
    string? completedAt;
|};

public type CreatePaymentRequest record {|
    string orderId;
    string customerId;
    int amountCents;
    PaymentMethod method;
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

public type ProcessedEvent record {|
    string eventId;
    string topic;
    string consumerGroup;
|};