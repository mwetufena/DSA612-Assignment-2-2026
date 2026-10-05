// Types for the Admin service — read-model projections (mini-CQRS).

public type OrderStats record {|
    string day;
    string restaurantId;
    int orderCount;
    int deliveredCount;
    int cancelledCount;
    int revenueCents;
|};

public type DeliveryStats record {|
    string day;
    string driverId;
    int assignedCount;
    int deliveredCount;
    float? avgDeliveryMinutes;
|};

public type PlatformSummary record {|
    int totalOrders;
    int totalDelivered;
    int totalCancelled;
    int totalRevenueCents;
    int totalPayments;
    int totalPaymentsSuccess;
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