public type Channel "EMAIL"|"SMS"|"PUSH"|"INAPP";

public type Notification record {|
    string id;
    string recipientId;
    Channel channel;
    string? subject;
    string body;
    string status;
    string? relatedEventId;
    string? correlationId;
    string createdAt;
    string? sentAt;
|};

public type CreateNotificationRequest record {|
    string recipientId;
    Channel channel;
    string? subject?;
    string body;
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