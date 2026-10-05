#!/usr/bin/env bash
# Creates all topics the platform expects + matching .dlq topics.
# Idempotent: re-running only creates missing topics.
set -e

BROKER="${KAFKA_BOOTSTRAP:-kafka:9092}"
PARTITIONS="${KAFKA_NUM_PARTITIONS:-3}"
REPLICATION=1

TOPICS=(
  "order.events"
  "payment.events"
  "delivery.events"
  "restaurant.events"
  "courier.events"
  "customer.events"
  "notification.events"
  "pricing.events"
  "promotion.events"
  "review.events"
  "settlement.events"
)

echo "Waiting for Kafka at $BROKER ..."
for i in {1..30}; do
  if kafka-topics --bootstrap-server "$BROKER" --list >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

for t in "${TOPICS[@]}"; do
  echo "Creating topic: $t"
  kafka-topics --bootstrap-server "$BROKER" \
    --create --if-not-exists \
    --topic "$t" \
    --partitions "$PARTITIONS" \
    --replication-factor "$REPLICATION" \
    --config retention.ms=604800000 \
    --config min.insync.replicas=1 || true

  echo "Creating DLQ: ${t}.dlq"
  kafka-topics --bootstrap-server "$BROKER" \
    --create --if-not-exists \
    --topic "${t}.dlq" \
    --partitions 1 \
    --replication-factor "$REPLICATION" \
    --config retention.ms=1209600000 || true
done

echo "Topics ready:"
kafka-topics --bootstrap-server "$BROKER" --list