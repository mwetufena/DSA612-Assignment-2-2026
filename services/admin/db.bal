import ballerina/sql;
import ballerinax/postgresql;
import ballerinax/postgresql.driver as _;

configurable string DB_HOST = "localhost";
configurable string DB_NAME = "admin_db";
configurable string DB_USER = "postgres";
configurable string DB_PASSWORD = "postgres";

final postgresql:Client db = check new (
    host = DB_HOST, database = DB_NAME, username = DB_USER, password = DB_PASSWORD);

// Upsert order stats projection (eventIdempotent via ON CONFLICT)
public function projectOrderEvent(string eventType, json payload, string occurredAt) returns sql:Error? {
    string day = occurredAt.substring(0, 10);
    string restaurantId = "00000000-0000-0000-0000-000000000000";
    if payload is map<json> && payload["restaurantId"] is string {
        restaurantId = <string>payload["restaurantId"];
    }

    if eventType == "order.created" {
        sql:ParameterizedQuery q = `INSERT INTO order_stats_daily
            (day, restaurant_id, order_count, delivered_count, cancelled_count, revenue_cents)
            VALUES (${day}::date, ${restaurantId}::uuid, 1, 0, 0, 0)
            ON CONFLICT (day, restaurant_id) DO UPDATE
            SET order_count = order_stats_daily.order_count + 1,
                updated_at = NOW()`;
        _ = check db->execute(q);
    } else if eventType == "order.delivered" {
        int revenue = 0;
        if payload is map<json> && payload["totalCents"] is int {
            revenue = <int>payload["totalCents"];
        }
        sql:ParameterizedQuery q = `INSERT INTO order_stats_daily
            (day, restaurant_id, order_count, delivered_count, cancelled_count, revenue_cents)
            VALUES (${day}::date, ${restaurantId}::uuid, 0, 1, 0, ${revenue})
            ON CONFLICT (day, restaurant_id) DO UPDATE
            SET delivered_count = order_stats_daily.delivered_count + 1,
                revenue_cents = order_stats_daily.revenue_cents + EXCLUDED.revenue_cents,
                updated_at = NOW()`;
        _ = check db->execute(q);
    } else if eventType == "order.cancelled" {
        sql:ParameterizedQuery q = `INSERT INTO order_stats_daily
            (day, restaurant_id, order_count, delivered_count, cancelled_count, revenue_cents)
            VALUES (${day}::date, ${restaurantId}::uuid, 0, 0, 1, 0)
            ON CONFLICT (day, restaurant_id) DO UPDATE
            SET cancelled_count = order_stats_daily.cancelled_count + 1,
                updated_at = NOW()`;
        _ = check db->execute(q);
    }
}

public function projectDeliveryEvent(string eventType, json payload, string occurredAt) returns sql:Error? {
    string day = occurredAt.substring(0, 10);
    string driverId = "00000000-0000-0000-0000-000000000000";
    if payload is map<json> && payload["driverId"] is string {
        driverId = <string>payload["driverId"];
    }

    if eventType == "delivery.assigned" {
        sql:ParameterizedQuery q = `INSERT INTO delivery_stats_daily
            (day, driver_id, assigned_count, delivered_count)
            VALUES (${day}::date, ${driverId}::uuid, 1, 0)
            ON CONFLICT (day, driver_id) DO UPDATE
            SET assigned_count = delivery_stats_daily.assigned_count + 1,
                updated_at = NOW()`;
        _ = check db->execute(q);
    } else if eventType == "delivery.delivered" {
        sql:ParameterizedQuery q = `INSERT INTO delivery_stats_daily
            (day, driver_id, assigned_count, delivered_count)
            VALUES (${day}::date, ${driverId}::uuid, 0, 1)
            ON CONFLICT (day, driver_id) DO UPDATE
            SET delivered_count = delivery_stats_daily.delivered_count + 1,
                updated_at = NOW()`;
        _ = check db->execute(q);
    }
}

public function orderStats(int maxRows) returns OrderStats[]|error {
    sql:ParameterizedQuery q = `SELECT day::text, restaurant_id::text,
        order_count, delivered_count, cancelled_count, revenue_cents
        FROM order_stats_daily ORDER BY day DESC LIMIT ${maxRows}`;
    stream<record {
        string day; string restaurant_id; int order_count;
        int delivered_count; int cancelled_count; int revenue_cents;
    }, sql:Error?> rs = db->query(q);
    OrderStats[] out = [];
    check from record {
        string day; string restaurant_id; int order_count;
        int delivered_count; int cancelled_count; int revenue_cents;
    } r in rs
        do {
            out.push({day: r.day, restaurantId: r.restaurant_id,
                orderCount: r.order_count, deliveredCount: r.delivered_count,
                cancelledCount: r.cancelled_count, revenueCents: r.revenue_cents});
        };
    return out;
}

public function deliveryStats(int maxRows) returns DeliveryStats[]|error {
    sql:ParameterizedQuery q = `SELECT day::text, driver_id::text,
        assigned_count, delivered_count, avg_delivery_minutes
        FROM delivery_stats_daily ORDER BY day DESC LIMIT ${maxRows}`;
    stream<record {
        string day; string driver_id; int assigned_count;
        int delivered_count; float? avg_delivery_minutes;
    }, sql:Error?> rs = db->query(q);
    DeliveryStats[] out = [];
    check from record {
        string day; string driver_id; int assigned_count;
        int delivered_count; float? avg_delivery_minutes;
    } r in rs
        do {
            out.push({day: r.day, driverId: r.driver_id,
                assignedCount: r.assigned_count, deliveredCount: r.delivered_count,
                avgDeliveryMinutes: r.avg_delivery_minutes});
        };
    return out;
}

// Public read-only proxy endpoints that hit other services' DBs
// (For simplicity we expose a few aggregates here; in real life these would be
// separate per-service aggregator endpoints.)

public function platformSummary() returns PlatformSummary|error {
    sql:ParameterizedQuery q = `SELECT
        COALESCE(SUM(order_count), 0)::int AS total_orders,
        COALESCE(SUM(delivered_count), 0)::int AS total_delivered,
        COALESCE(SUM(cancelled_count), 0)::int AS total_cancelled,
        COALESCE(SUM(revenue_cents), 0)::int AS total_revenue,
        0 AS total_payments,
        0 AS total_payments_success
        FROM order_stats_daily`;
    record {
        int total_orders; int total_delivered; int total_cancelled;
        int total_revenue; int total_payments; int total_payments_success;
    }? r = check db->queryRow(q);
    if r is () {
        return {totalOrders: 0, totalDelivered: 0, totalCancelled: 0,
            totalRevenueCents: 0, totalPayments: 0, totalPaymentsSuccess: 0};
    }
    return {totalOrders: r.total_orders, totalDelivered: r.total_delivered,
        totalCancelled: r.total_cancelled, totalRevenueCents: r.total_revenue,
        totalPayments: 0, totalPaymentsSuccess: 0};
}

public function isEventProcessed(string eventId, string group) returns boolean|error {
    sql:ParameterizedQuery q = `SELECT 1 AS v FROM processed_events
        WHERE event_id = ${eventId}::uuid AND consumer_group = ${group}`;
    record {|int v;|}? r = check db->queryRow(q);
    return r is record {|int v;|};
}

public function recordProcessed(string eventId, string topic, string group) returns sql:Error? {
    sql:ParameterizedQuery q = `INSERT INTO processed_events (event_id, topic, consumer_group)
        VALUES (${eventId}::uuid, ${topic}, ${group})
        ON CONFLICT (event_id) DO NOTHING`;
    _ = check db->execute(q);
}

public function healthCheck() returns boolean|error {
    sql:ParameterizedQuery q = `SELECT 1 AS v`;
    record {|int v;|}|error r = db->queryRow(q);
    return r is record {|int v;|};
}