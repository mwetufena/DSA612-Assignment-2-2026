# Architecture Notes — Decision Log

This document captures **why** the project looks the way it does. It complements `README.md`, which is the *what*.


## 1. Why Ballerina?

Ballerina is purpose-built for network services. In one language file you can:

- Declare an HTTP service with strongly-typed resources
- Consume a Kafka topic with a typed service
- Query a database with parameterized SQL
- Use `transaction { … }` blocks for atomicity

That removes an enormous amount of glue code that would otherwise obscure the distributed-systems patterns we're trying to *teach*. Every file in `services/*/service.bal` directly expresses the pattern; nothing is hidden behind framework abstractions.


## 2. Why a relational DB per service (not shared)?

Sharing one database across services creates a hidden coupling: every schema change becomes a coordinated deployment. By giving each service its own logical DB (in a single physical Postgres instance for this demo), we get:

- True service autonomy
- Independently evolvable schemas
- No cross-service joins — the only way to "join" data is through events
- Easy future split: move `delivery_db` to its own Postgres cluster with zero code changes

The price is *eventual* consistency — which is what the saga and outbox patterns solve.


## 3. Why the Transactional Outbox?

Writing to a DB and a Kafka topic in two separate steps guarantees at least one of three failure modes:

1. DB write succeeded, Kafka send failed → event lost
2. DB write failed, Kafka send succeeded → event refers to a row that doesn't exist
3. Both succeeded but the client retried → duplicate events without dedupe keys

The outbox pattern writes the event into a **DB table** in the same transaction as the business write. A separate relay process polls unpublished rows and forwards them to Kafka. With `FOR UPDATE SKIP LOCKED`, multiple relay replicas can run without stepping on each other.

**Costs:** a few ms of relay latency, an extra row per event. Worth it.


## 4. Why a Saga (not 2PC)?

A single customer order touches **at least four** databases (order, payment, restaurant inventory, delivery). A 2-phase commit across all of them is:

- Slow (locks held across network)
- Brittle (coordinator crash leaves everyone stuck)
- Increasingly unsupported in modern NewSQL/NoSQL databases

A saga replaces atomicity with **compensating actions**: each local commit is followed by either the next step or a rollback of the previous step. For example:

```
CONFIRMED → PaymentDeclines → CancelReservation → CANCELLED
```

The saga orchestrator (Order Service) emits one event per step, and each event handler either advances the saga or triggers the compensating action.

---

## 5. Why optimistic locking?

The Order state machine is racy: two clients could try to confirm or cancel the same order simultaneously. Without locking:

- Both reads see `state=CREATED`
- Both writes set `state=CONFIRMED` (or `CANCELLED`)
- The second write silently overwrites the first → lost intent

Optimistic locking (`UPDATE … WHERE version=N`) means only the writer who holds the current version succeeds. The other gets a `0 rows affected` signal and retries with fresh data.

For this demo we use Postgres's row versioning via an explicit `version` column; in production you'd consider `SERIALIZABLE` isolation or advisory locks for very hot aggregates.

---

## 6. Why inventory reservations + TTL?

Naive stock management ("decrement on order, refund on cancel") oversells during traffic spikes because the "decrement" and the "check there was enough" are separate.

Reserving stock up front **and** putting a TTL on the reservation guarantees:

- Two customers can't both grab the last item
- A reservation that never gets confirmed (cart abandoned, network died) auto-expires
- The kitchen only sees confirmed orders

---

## 7. Why mini-CQRS for Admin?

CQRS — separating reads from writes — is overkill for many systems. Here it's worthwhile because:

- Reports don't need transactional consistency with order writes
- Reads on aggregate tables (`SUM(revenue_cents) GROUP BY day`) get expensive at scale
- A separate read model can be re-built from scratch by replaying the event log

For this demo we keep it minimal: two aggregate tables in `admin_db`, fed by a Kafka consumer in `consumer.bal`. The same consumer would in production live in a stream-processing engine (Flink, Kafka Streams, ksqlDB).

---

## 8. Why events instead of synchronous chains?

An alternative to events is *choreography*: every service directly calls every other service it depends on. That works for 2-3 services and collapses at 7:

- Every change to the contract of one service requires touching every caller
- A failure in a downstream service can back-pressure into an upstream service
- The order of operations becomes implicit and brittle

Events invert the dependency: each service **emits** events it owns and **consumes** events it cares about. No service knows who consumes its events. The Order Service can change its state machine without touching Customer, Delivery, or Admin.

---

## 9. Why a simple HTML GUI?

The user asked for "a well-crafted graphical user interface". Three options:

| Option              | Pros                                     | Cons                                             |
|---------------------|------------------------------------------|--------------------------------------------------|
| React SPA           | Familiar, easy to build                  | Adds a 200MB `node_modules` and a build pipeline |
| Vue SPA             | Same                                     | Same                                             |
| Vanilla HTML+CSS+JS | Zero build, instant reload, easy to read | More boilerplate for complex UIs                 |

We chose **vanilla**. The total surface area is small enough (~7 tabs, ~10 forms) that the boilerplate is manageable, and the entire frontend fits in `gui/index.html + styles.css + app.js` — no toolchain, no `npm install`, no surprises in VS Code. The dark theme uses the GitHub palette for a familiar developer feel.

---

## 10. Why Docker Compose (not Kubernetes)?

For an educational artifact, Compose is the right level:

- One file to read (`docker-compose.yml`)
- Healthchecks with `depends_on.condition: service_healthy` give us ordered startup
- No need to learn Helm/Istio/ingress to run it

---

## 11. Decisions deferred (intentional simplifications)

- **No real auth.** Every endpoint is open. In production: JWT + mTLS + per-service API keys.
- **No real payment processor.** Payment Service rolls a die and emits `payment.success` or `payment.failed`. In production: integrate Stripe / Adyen / etc., and use a circuit breaker on the HTTP call.
- **No real email/SMS.** Notification Service records the alert and emits a `notification.created` event; a downstream worker would do actual delivery.
- **No tracing.** `correlationId` is propagated but we don't collect spans. Ballerina's `ballerina/observe` would hook into OpenTelemetry with `observabilityIncluded=true` in `Ballerina.toml`.
- **No metrics.** Same — add `--observability-included=true` at build time and Prometheus will scrape `/metrics` natively.
- **No strict per-user isolation in `address_id`.** The Order accepts any UUID; in production you'd validate that the address belongs to the authenticated customer.

---

## 12. How to read the code

A guided tour, in dependency order:

1. `infra/postgres/init/03-order.sql` — read the schema, especially the `orders`, `outbox_events`, and `saga_steps` tables
2. `services/order/types.bal` — the `OrderState` enum is the heart of the state machine
3. `services/order/db.bal` — `transitionState(...)` enforces optimistic locking on every state move
4. `services/order/service.bal` — every resource function maps to one legal transition; illegal transitions return `ILLEGAL_STATE`
5. `services/order/relay.bal` — the outbox relay
6. `services/payment/service.bal` — the simulated processor; emits `payment.success` or `payment.failed`
7. `services/admin/consumer.bal` — how a consumer pulls events and writes projections idempotently

Once you can trace a single `POST /orders` request through Order → Restaurant → Payment → Delivery → Notification → Admin, you understand the entire platform.