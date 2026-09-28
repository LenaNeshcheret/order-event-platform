# Kafka Learning Project — Architecture

## Purpose

This repository is a hands-on Apache Kafka learning project. The business domain is intentionally simple: an e-commerce order lifecycle. Business logic exists only to create realistic Kafka problems.

Primary goal: understand Kafka behavior, configuration, failure modes, and trade-offs through experiments.

## Technology baseline

- Java 21
- Spring Boot 4.1.1
- Spring for Apache Kafka managed by Spring Boot
- Apache Kafka 4.3.1
- Maven multi-module build
- Docker Compose
- Kafka in KRaft mode; no ZooKeeper
- Kafbat UI v1.5.0 for visual inspection
- JSON events initially
- No database until the transactional-outbox lab

Do not override Spring-managed Kafka client versions unless a later lab explicitly requires it.

## Business domain

A customer submits an order. The order must eventually be paid, inventory must be reserved, and successful orders can be fulfilled and notified.

The final learning system is:

```text
Client
  |
  | POST /orders
  v
order-service
  |
  | order.created        key = orderId
  v
Kafka
  +---------------------> payment-service
  |                         |
  |                         +--> payment.completed
  |                         +--> payment.failed
  |
  +---------------------> inventory-service
  |                         |
  |                         +--> inventory.reserved
  |                         +--> inventory.rejected
  |
  +---------------------> audit/analytics consumer

payment.* ---------+
inventory.* -------+----> fulfillment-service (Kafka Streams)
                          |
                          +--> order.ready-for-shipment
                                      |
                                      v
                              notification-service
```

This is the target architecture, not Lab 1 scope.

## Why this domain fits Kafka

The same `orderId` naturally demonstrates record keys and per-partition ordering. Payment and inventory naturally demonstrate independent consumer groups. Failures naturally demonstrate redelivery, retries, poison messages, DLTs, and idempotency. Joining payment and inventory outcomes naturally demonstrates Kafka Streams. Order state naturally demonstrates compacted topics/KTables. A later persistence requirement naturally introduces the dual-write problem and transactional outbox.

## Planned repository shape

```text
order-event-platform/
├── AGENTS.md
├── ARCHITECTURE.md
├── LEARNING_PLAN.md
├── README.md
├── pom.xml
├── docker-compose.yml
├── common-events/
├── order-service/
├── payment-service/
├── inventory-service/          # later lab
├── fulfillment-service/        # later lab
└── notification-service/       # later lab
```

Only create modules required by the active lab.

## Event naming

Use lower-case dot-separated topic names.

Planned business topics:

```text
order.created
payment.completed
payment.failed
inventory.reserved
inventory.rejected
order.ready-for-shipment
order.state
```

Retry/DLT topics are introduced only in their corresponding lab.

## Event envelope

Start with JSON and a small explicit envelope:

```json
{
  "eventId": "uuid",
  "eventType": "OrderCreated",
  "eventVersion": 1,
  "occurredAt": "2026-09-27T10:00:00Z",
  "correlationId": "uuid",
  "payload": {}
}
```

Keep `orderId` in the business payload and use `orderId` as the Kafka record key for order-related events.

Do not create an elaborate shared framework around the envelope. The project should make Kafka concepts visible.

## Initial event contracts

### OrderCreated

```json
{
  "eventId": "...",
  "eventType": "OrderCreated",
  "eventVersion": 1,
  "occurredAt": "...",
  "correlationId": "...",
  "payload": {
    "orderId": "...",
    "customerId": "customer-123",
    "items": [
      {
        "productId": "product-1",
        "quantity": 2,
        "unitPrice": 19.99
      }
    ],
    "totalAmount": 39.98,
    "currency": "EUR"
  }
}
```

### PaymentCompleted / PaymentFailed

Contain at minimum `orderId`, `paymentId`, amount/currency, and a result/reason appropriate to the event.

## Partitioning rule

Unless a lab intentionally changes it:

```text
Kafka record key = orderId
```

This allows all events for the same order on a given topic to map deterministically to a partition and gives us a concrete way to study ordering.

Do not manually calculate partition numbers in application code.

## Consumer groups

Each independent business responsibility uses a different consumer group.

Examples:

```text
payment-service     -> payment-service
inventory-service   -> inventory-service
notification-service -> notification-service
```

Multiple instances of the same service share the same group ID so we can study partition assignment and rebalancing.

## Topic creation

For learning, prefer explicit topic definitions (`NewTopic` beans or an infrastructure mechanism visible in code) rather than silently depending on broker auto-creation.

Early labs should normally use:

```text
partitions = 3
replication-factor = 1
```

A later lab will use a multi-broker cluster to study replication, ISR, leader election, and durability. Do not pretend replication factor 1 teaches broker fault tolerance.

## Delivery semantics roadmap

The project deliberately evolves through different semantics:

1. Basic delivery and default offset behavior.
2. At-least-once processing and duplicate observation.
3. Idempotent processing.
4. Kafka transactional read-process-write and exactly-once semantics where applicable.
5. DB + Kafka dual-write problem and transactional outbox.

Do not call an entire end-to-end business workflow "exactly once" merely because a Kafka producer or a read-process-write segment uses transactions.

## Database policy

No database in the initial project.

In early labs, in-memory structures may be used only when needed to demonstrate behavior. Their loss on restart is acceptable and should be documented.

PostgreSQL is introduced late specifically for:

- the dual-write problem;
- DB transaction boundaries;
- transactional outbox;
- eventual publication to Kafka.

## Observability

Kafbat UI is included for inspecting:

- topics;
- partitions;
- messages and keys;
- consumer groups;
- offsets;
- lag.

Later labs add Spring Boot Actuator/Micrometer metrics where useful. Logs should always include topic, partition, offset, key, consumer group when practical.

## Testing philosophy

Prefer tests that demonstrate Kafka behavior rather than mocking Kafka everywhere.

Progression:

- unit tests for pure business transformations;
- Spring Kafka tests where useful;
- integration tests with a real Kafka container later;
- Docker Compose manual experiments remain first-class learning exercises.

## Explicit non-goals

Until a lab requests them, do not add:

- PostgreSQL/JPA;
- authentication/authorization;
- Kubernetes;
- cloud Kafka;
- Schema Registry;
- Avro/Protobuf;
- Kafka Connect;
- distributed tracing stack;
- elaborate DDD abstractions;
- generic messaging frameworks hiding Kafka APIs.
