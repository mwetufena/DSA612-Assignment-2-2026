import ballerina/http;

service / on new http:Listener(8090) {

    resource function get healthz() returns http:Ok { return http:OK; }

    resource function get readyz() returns http:Ok|http:ServiceUnavailable {
        boolean|error ok = healthCheck();
        if ok is boolean && ok { return http:OK; }
        return http:SERVICE_UNAVAILABLE;
    }

    resource function get reports/orders(int maxRows = 100) returns OrderStats[]|error {
        return check orderStats(maxRows);
    }

    resource function get reports/deliveries(int maxRows = 100) returns DeliveryStats[]|error {
        return check deliveryStats(maxRows);
    }

    resource function get reports/summary() returns PlatformSummary|error {
        return check platformSummary();
    }
}