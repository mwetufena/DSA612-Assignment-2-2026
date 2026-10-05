## 🔨 Mjölnir — Distributed Food Delivery Platform
---

## > An end-to-end distributed system built with Ballerina microservices, Apache Kafka event streaming, PostgreSQL per-service databases, Docker Compose orchestration, and a modern web GUI.
## > Built to apply real-world distributed-systems skills: microservices, event-driven choreography, transactional outbox, saga pattern, optimistic concurrency, idempotency, and observability.


## 📚 Table of Contents

1. [What is this project?](#-what-is-this-project)
2. [Architecture at a glance](#-architecture-at-a-glance)
3. [Distributed-systems patterns implemented](#-distributed-systems-patterns-implemented)
4. [Tech stack](#-tech-stack)
5. [Project layout](#-project-layout)
6. [Prerequisites](#-prerequisites)
7. [Run it locally](#-run-it-locally)
8. [Open the GUI](#-open-the-gui)
9. [Demo walkthrough](#-demo-walkthrough)
10. [Inspecting Kafka & PostgreSQL](#-inspecting-kafka--postgresql)
11. [API surface](#-api-surface)
12. [Event topics](#-event-topics)
13. [Configuration](#-configuration)
14. [Troubleshooting](#-troubleshooting)
15. [What you can extend next](#-what-you-can-extend-next)
16. [Why each pattern matters (deep dive)](#-why-each-pattern-matters-deep-dive)


## 🎯 What is this project?

* ## This is a production-style distributed food-delivery platform with seven independent microservices that coordinate through asynchronous Kafka events and synchronous HTTP calls. Every actor in the real world — Restaurants, Customers, Drivers, Payments, Notifications, Admin — has its own service, its own database, and its own event channels.

---
---
## 🏗️ Architecture at a glance

```
┌──────────────────────────────────────────────────────────────────────────┐
│                       Browser  (http://localhost:8080)                   │
│                              index.html / app.js                         │
└──────────────────────────────────────────────────────────────────────────┘
                                       │ REST
                                       ▼
┌─────────────┬──────────────┬──────────────┬───────────────┬─────────────┐
│  customer   │  restaurant  │    order     │   payment     │   delivery  │
│  :8081      │  :8082       │    :8083     │   :8084       │   :8085     │
└─────────────┴──────────────┴──────────────┴───────────────┴─────────────┘
       │                │              │              │              │
       │   ┌────────────┴──────────────┴──────────────┴──────────────┤
       │   │                                                         │
       ▼   ▼                                                         ▼
┌──────────────────────────────────────┐         ┌──────────────────────────────┐
│  notification  :8086  │  admin  :8087│         │  Apache Kafka  (3.0.0)       │
│   (consumer)  (proj.) │              │◀────── | Zookeeper                    |
└──────────────────────────────────────┘         │  12 topics + .dlq            │
                                                 └──────────────────────────────┘
        │             │              │                     │
        ▼             ▼              ▼                     ▼
┌──────────────────────────────────────────────────────────────────────────┐
│   PostgreSQL 18 — one logical database per service (init scripts)        │
│   customer_db | restaurant_db | order_db | payment_db                    │
│   delivery_db  | notification_db | admin_db                              │
│   Each DB: aggregates + outbox_events + idempotency_keys + processed_*   │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Microservices

| # | Service        | Port | Database        | Purpose                                                                        |
|---|----------------|------|-----------------|--------------------------------------------------------------------------------|
| 1 | Customer       | 8081 | customer_db     | User accounts, addresses, registration, profile management                     |
| 2 | Restaurant     | 8082 | restaurant_db   | Menus, real-time inventory, opening hours, TTL reservations                    |
| 3 | Order          | 8083 | order_db        | Central order state machine + saga orchestrator (8 states, optimistic locking) |
| 4 | Payment        | 8084 | payment_db      | Simulated processor, configurable failure rate, txn refs                       |
| 5 | Delivery       | 8085 | delivery_db     | Driver registry, assignment, status tracking, location updates                 |
| 6 | Notification   | 8086 | notification_db | Multi-channel alerts (EMAIL/SMS/PUSH/INAPP), event-driven dispatch             |
| 7 | Admin          | 8087 | admin_db        | Read-model projections (mini-CQRS), platform reports, daily aggregates         |



---

## 🧩 Distributed-systems patterns implemented

## This is the heart of the project:

| Pattern                                     | Where it lives                | Why it matters |
|---------------------------------------------|-------------------------------|---|
| Transactional Outbox                        | services/*/db.bal + relay.bal     | Atomicity between DB writes and event publishing — eliminates dual-write inconsistency. |
| Idempotent HTTP                             | Idempotency-Key header on POST /customers, POST /orders | Client retries (network blips, page reloads) cannot create duplicates. |
| Idempotent Consumers                        | processed_events` table in every consumer DB | Kafka's at-least-once delivery means duplicates will happen — dedupe by `eventId`. |
| Dead Letter Queue (DLQ)                     | *.dlq` topics auto-created alongside every domain topic | Poison messages never block the partition. |
| Saga Orchestration                          | services/order/service.bal` | Distributed transaction across Payment/Restaurant/Delivery with compensating actions. |
| Optimistic Locking                          | version` column on `orders`, `payments`, `deliveries` | Concurrent state transitions can never silently corrupt the state machine. |
| State-machine guards                        | transitionState(expected, next, ...)` | Illegal transitions (e.g. `PREPARING → CANCELLED`) are rejected server-side. |
| Inventory Reservation + TTL                 | inventory_reservations` table with `expires_at` | Stock is reserved on confirm, auto-released on payment failure. |
| Database-per-service                        | Seven separate PostgreSQL DBs | No cross-service joins — true data autonomy. |
| Read Models / mini-CQRS                     | admin_db.order_stats_daily`, `delivery_stats_daily` | Admin reports are fed by event projections, not by querying OLTP tables. |
| Event Versioning                            | schemaVersion` in every envelope | Consumers can evolve independently without breaking older producers. |
| Health endpoints                            | /healthz` liveness, `/readyz` readiness per service | Docker Compose uses these to gate startup ordering. |
| Timeouts + circuit-breaker-friendly clients | All inter-service HTTP clients declare `timeout: 5` | Avoid cascading failures during partial outages. |
| Idempotency via headers                     | eventId` UUID in Kafka headers | Consumers can dedupe even if the payload is malformed. |
| UTC timestamps everywhere                   | time:utcToString(time:utcNow())` | No timezone bugs across regions. |
| Config via env vars                         | configurable` declarations in every service | All config is overridable; nothing baked into images. |


---

## 🧰 Tech stack

| Concern         | Choice                                    |
|-----------------|-------------------------------------------|
| Language        | Ballerina 2201.13.5 (Swan Lake Update 13) |
| Backend runtime | OpenJDK 21                                |
| Streaming       | Apache Kafka 3.0.0 + Zookeeper            |
| Database        | PostgreSQL 18                             |
| Orchestration   | Docker Compose v5.3.1, Docker Engine 29   |
| GUI             | Vanilla HTML / CSS / JS (no build step)   |
| Reverse proxy   | nginx (serving the GUI)                   |

---


## 🗂️ Project layout

```
Mjölnir/
├── README.md                       # ← you are here
├── docker-compose.yml              # infra + 7 services + GUI
├── .env.example                    # environment-variable template
├── Ballerina.toml                  # (legacy; each service has its own)
│
├── infra/
│   ├── postgres/init/              # 01-customer.sql … 07-admin.sql
│   │                               # creates per-service DBs + tables
│   └── kafka/topics.sh             # creates 12 topics + matching .dlq
│
├── services/                       # one Ballerina package per service
│   ├── customer/                   # Ballerina.toml + Dockerfile + *.bal
│   ├── restaurant/
│   ├── order/                      # most complex: state machine + saga
│   ├── payment/                    # simulated processor with random failures
│   ├── delivery/                   # driver registry + assignment
│   ├── notification/
│   └── admin/                      # consumer + read-model projections
│
├── gui/
│   ├── index.html                  # single-page UI
│   ├── styles.css                  # dark GitHub-style theme
│   ├── app.js                      # fetch-based REST client
│   └── nginx.conf                  # SPA-friendly nginx config
│
└── docs/
    └── architecture.md             # design notes + decision log
```

---

## ✅ Prerequisites

* Ballerina 2201.13.5 (Swan Lake Update 13)
* OpenJDK 21.0.12.1 2026-08-18 LTS
* Apache Kafka 3.0.0
* PostgreSQL 18.6
* Docker Engine 29.6.2
* Docker Compose version v5.3.1
* git 2.55.0.windows.5

___


## ▶️ Run it locally

1. Clone & configure

   ## `powershell:`
   * cd C:\Users\USER\Desktop\Mjölnir
   * Copy-Item .env.example .env       # (optional; defaults also works)
   ---

---
2. Build & start everything

    ## `powershell:`
    * docker compose up --build -d
    ___

---

## This will:

1. Pull Confluent Kafka + Zookeeper + Postgres 18 images
2. Build each Ballerina service image (≈ 5–8 min the first time)
3. Initialize PostgreSQL with seven per-service databases
4. Wait for Kafka health, then create 12 topics + their .dlq twins
5. Start all 7 services (each with a /healthz liveness check)
6. Start the GUI behind nginx on port 8080

---

## Watch the logs:

## `powershell`:
```
* docker compose logs -f kafka-init            # topic provisioning
* docker compose logs -f order                 # Order saga transitions
* docker compose logs -f postgres              # DB init scripts
```
---


## 3. Verify if everything is up

`powershell:`
* curl http://localhost:8081/healthz     # customer
* curl http://localhost:8082/healthz    # restaurant
* curl http://localhost:8083/healthz    # order
* curl http://localhost:8084/healthz    # payment
* curl http://localhost:8085/healthz    # delivery
* curl http://localhost:8086/healthz    # notification
* curl http://localhost:8087/healthz    # admin
---

All should return `200 OK`.
---

## 🌐 Open the GUI

Browse to:
http://localhost:8080
---


## The dashboard shows:

- `Service health` (live green/red indicators for each service)
- `Customers` — create + list (uses Idempotency-Key if provided)
- `Restaurants & menus` — create restaurants, add menu items with stock
- `Orders` — place an order, walk it through the state machine (CREATED → CONFIRMED → PREPARING → READY → OUT_FOR_DELIVERY → DELIVERED)
- `Drivers` — register drivers; the demo auto-assigns available drivers when an order is dispatched
- `Notifications` — send multi-channel alerts
- `Reports` — live read-model projections maintained by the Admin consumer

---

## 🔍 Inspecting Kafka & PostgreSQL

## Tail a Kafka topic

`powershell`
* `docker exec mjolnir-food-delivery-kafka-1 kafka-console-consumer --bootstrap-server kafka:9092 --topic order.events --from-beginning --max-messages 20`

---
* The other topics follow the same recipe:
  `payment.events`, `delivery.events`, `restaurant.events`, `customer.events`, `notification.events`, plus *.dlq for poison messages.

----


## Inspect the outbox:

`powershell:`
 * docker exec mjolnir-food-delivery-postgres-1
 * psql -U postgres -d order_db -c
 * "SELECT id, topic, event_id, correlation_id, published_at FROM outbox_events ORDER BY id DESC LIMIT 10;"

---
## Unpublished rows show published_at = NULL — the relay retries them every second.
---

## Check event idempotency

`powershell:`
 * docker exec mjolnir-food-delivery-postgres-1
 * psql -U postgres -d admin_db -c
 * "SELECT COUNT(*) FROM processed_events;"
---
---
## Failed / dead-lettered messages
---
`powershell:`
* docker exec mjolnir-food-delivery-kafka-1
* kafka-console-consumer --bootstrap-server kafka:9092 --topic order.events.dlq --from-beginning

---
---

## 📡 API surface
---

## Customer (http://localhost:8081)

| Method | Path                      | Notes                           |
|--------|---------------------------|---------------------------------|
| GET    | /healthz · /readyz        | Liveness & readiness probe      |
| POST   | /customers                | Accepts Idempotency-Key header  |
| GET    | /customers                | Browse all                      |
| GET    | /customers/{id}           | Single                          |
| GET    | /customers/{id}/addresses | Per-customer address book       |
| POST   | /customers/{id}/addresses | Add an address                  |

---
---


## Restaurant (http://localhost:8082)

| Method | Path                          | Notes                                          |
|--------|-------------------------------|------------------------------------------------|
| GET    | /restaurants                  | All                                            |
| POST   | /restaurants                  | Create (emits restaurant.created)              |
| GET    | /restaurants/{id}             | Single                                         |
| POST   | /restaurants/{id}/menu        | Add menu item (emits menu.item.added)          |
| GET    | /restaurants/{id}/menu        | Browse                                         |
| POST   | /inventory/reserve            | Internal — used by Order saga                  |
| POST   | /inventory/release/{orderId}  | Internal — compensation                        |
| POST   | /inventory/confirm/{orderId}  | Internal — convert reservation → stock out     |
---
---



## Order (http://localhost:8083)

| Method | Path                              | Notes                                            |
|--------|-----------------------------------|--------------------------------------------------|
| POST   | /orders                           | dempotency-Key supported                         |
| GET    | /orders/{id}                      | Order + items                                    |  
| POST   | /orders/{id}/confirm              | CREATED → CONFIRMED (reserves stock)             |
| POST   | /orders/{id}/startPreparing       | CONFIRMED → PREPARING                            |
| POST   | /orders/{id}/ready                | PREPARING → READY                                |
| POST   | /orders/{id}/dispatch             | READY → OUT_FOR_DELIVERY (assigns driver)        |
| POST   | /orders/{id}/deliver              | OUT_FOR_DELIVERY → DELIVERED (commits inventory) |
| POST   | /orders/{id}/cancel               | CREATED/CONFIRMED → CANCELLED (releases stock)   |

---
---


## Payment (http://localhost:8084)

| Method | Path                           | Notes                                        |
|--------|--------------------------------|----------------------------------------------|
| POST   | /payments                      | Simulated processor; ~5% failure rate        |
| GET    | /payments/{id}                 | Look up by ID                                |
| GET    | /payments                      | Most recent 50                               |
---
---



## Delivery (http://localhost:8085)

| Method | Path                               | Notes                                     |
|--------|------------------------------------|-------------------------------------------|
| GET    | /drivers                           | All drivers                               |
| POST   | /drivers                           | Register driver                           |
| POST   | /deliveries/assign                 | Internal — saga calls this on dispatch    |
| GET    | /deliveries/{id}                   | Look up                                   |
| GET    | /deliveries/findByOrder/{orderId}  | Find by order id                          |
| POST   | /deliveries/{id}/status            | Update to PICKED_UP or DELIVERED          |

---
---


## Notification (http://localhost:8086)

| Method | Path                                | Notes                                          |
|--------|-------------------------------------|------------------------------------------------|
| POST   | /notifications                      | Send multi-channel alert                       |
| GET    | /notifications                      | All recent                                     |
| GET    | /notifications/recipient/{id}       | Per-recipient feed                             |


---
---

## Admin (http://localhost:8087)

| Method | Path                           | Notes                                          |
|--------|--------------------------------|------------------------------------------------|
| GET    | /reports/orders                | Daily order aggregates (projection)            |
| GET    | /reports/deliveries            | Daily delivery aggregates (projection)         |
| GET    | /reports/summary               | Platform-wide totals                           |


---
---

## 📨 Event topics

* ## 12 canonical topics, each with a matching *.dlq for poison-pill protection:

| Topic                  | Published by               | Consumed by                                  |
|------------------------|----------------------------|----------------------------------------------|
| order.events           | Order                      | Restaurant, Payment, Notification, Admin     |
| payment.events         | Payment                    | Order, Notification, Admin                   |
| delivery.events        | Delivery                   | Notification, Admin                          |
| restaurant.events      | Restaurant                 | Notification, Admin                          |
| customer.events        | Customer                   | Notification, Admin                          |
| courier.events         | Delivery                   | Notification, Admin                          |
| notification.events    | Notification               | Admin                                        |
| pricing.events         | (extension point)          | Admin                                        |
| promotion.events       | (extension point)          | Admin                                        |
| review.events          | (extension point)          | Admin                                        |
| settlement.events      | (extension point)          | Admin                                        |
---
## Every event uses a canonical envelope:

---


`json format:`
```
{
  "eventId": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "correlationId": "0a3b…",
  "schemaVersion": 1,
  "eventType": "order.created",
  "aggregateType": "Order",
  "aggregateId": "…",
  "topic": "order.events",
  "occurredAt": "2026-09-24T10:30:00Z",
  "payload": { … }
}
```

---
* ## The envelope is also duplicated in the Kafka record's headers (eventId, correlationId, schemaVersion) so consumers can dedupe even on malformed bodies.

---
---

## ⚙️ Configuration

## Every service reads its configuration from environment variables (mapped via Ballerina 'configurable'):

| Var                    | Default                 | Used by                                  |
|------------------------|-------------------------|------------------------------------------|
| KAFKA_BOOTSTRAP        | localhost:9092          | Every service                            |
| DB_HOST                | localhost               | Every service                            |
| DB_PORT                | 5432                    | Every service                            |
| DB_NAME                | <service>_db            | Every service                            |
| DB_USER / DB_PASSWORD  | postgres / postgres     | Every service                            |
| SERVICE_PORT           | 8090                    | Every service                            |
| PAYMENT_FAILURE_RATE   | 0.05                    | Payment (5% simulated decline rate)      |
| RESTAURANT_URL         | http://restaurant:8090  | Order (saga → reserve/release/confirm)   |
| PAYMENT_URL            | http://payment:8090     | Order (saga → request charge)            |
| DELIVERY_URL           | http://delivery:8090    | Order (saga → assign driver)             |
| KAFKA_GROUP            | admin-projections       | Admin (consumer group)                   |
| RELAY_INTERVAL_SECONDS | 1                       | Every outbox relay                       |
| RELAY_BATCH            | 50                      | Every outbox relay                       |
---
* Override any of them in .env or docker-compose.yml.
---
---
## 🛠️ Troubleshooting

| Symptom                                           | Likely cause / fix                                                                       |
|---------------------------------------------------|------------------------------------------------------------------------------------------|
| Connection refused to a service                   | Check docker compose ps — service may not have finished building                         |
| Outbox events stuck with NULL published_at        | Relay couldn't reach Kafka; verify KAFKA_BOOTSTRAP and kafka container health            |
| Order stuck in CREATED                            | Confirm wasn't called — the saga is manually driven from the GUI to teach the flow       |
| Payment always fails                              | Adjust PAYMENT_FAILURE_RATE (5% by default; you got unlucky on a small sample)           |
| GUI shows a red dot for a service                 | Click the dashboard refresh; if persistent, docker compose logs <service>                |
| Build fails on Ballerina version                  | Verify bal version returns 2201.13.5                                                     |
| Module ballerinax/kafka not found                 | Run bal build once locally so Ballerina caches dependencies (~/.ballerina/repositories)  |

---
---

## 🧠 Why each pattern matters (deep dive)

* ## Transactional Outbox
  * `Problem`: "I wrote the row, then sent the Kafka event — and the network died."
  * Without outbox, you either lose the event or double-write. With outbox, the row and the event sit in the same DB transaction. A relay     (relay.bal in every service) polls unpublished rows, sends them, and marks them. FOR UPDATE SKIP LOCKED lets multiple replicas of the relay coexist safely.

* ## Saga orchestration
  * `Problem`: A food order touches Customer, Restaurant, Payment, and Delivery. Distributed transactions (XA) don't work in modern infra.
  * `Solution`: The Order Service is the saga orchestrator. Each step is a local transaction; failures trigger compensating actions (e.g., release reserved inventory if the card declines). Compensation is first-class, not an afterthought.

* ## Idempotency
  * `Problem`: Kafka guarantees at-least-once. The same payment.success event may arrive twice.
  * `Solution`: Every consumer maintains a processed_events(eventId, topic, consumer_group) table. Every HTTP write endpoint accepts an Idempotency-Key header — repeat requests get the cached response.

* ## Optimistic locking
  * `Problem`: Two clients simultaneously try to advance an order from READY to OUT_FOR_DELIVERY.
  * `Solution`: UPDATE orders SET state='OUT_FOR_DELIVERY' WHERE state='READY' AND version=N. If 0 rows updated, the second writer loses and retries with the latest version.

* ## State-machine guards
  * `Problem`: Without guardrails, a buggy client could set DELIVERED → PREPARING.
  * `Solution`: The Order Service rejects illegal transitions server-side and emits order.rejected events when guards fire.

* ## Inventory reservation + TTL
  * `Problem`: 100 customers place orders in 5 seconds. Without reservation, the restaurant oversells.
  * `Solution`: inventory_reservations(orderId, menuItemId, qty, expires_at). On order confirmation we 'reserve' (don't decrement). On payment success we 'confirm' (decrement). On payment failure / timeout we 'release' (auto-cleaner sweeps expired rows).

* ## Read models (mini-CQRS)
  * `Problem`: Admin reports that scan OLTP tables during business hours degrade the whole platform.
  * `Solution`: A separate admin_db writes only via Kafka consumer projections. Reports are O(1) SELECT day=... .

* ## DLQ
  * `Problem`: A single malformed event blocks the partition forever.
  * `Solution`: A consumer that fails N times routes the record to *.dlq instead of retrying blindly.
---

---


## 📖 Further reading

- docs/architecture.md — design decisions and trade-offs
- infra/postgres/init/*.sql — full schema for each service
- infra/kafka/topics.sh — topic provisioning
- Ballerina docs: https://ballerina.io/learn/
- Apache Kafka docs: https://kafka.apache.org/documentation/
---
---