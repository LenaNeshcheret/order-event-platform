# Kafka Learning Project

A progressive, hands-on Apache Kafka project using Java and Spring Boot.

## Purpose

The goal is not to build a feature-rich e-commerce application. The goal is to learn Kafka by observing it under normal operation, scaling, failure, replay, retries, transactions, stream processing, schema changes, broker failure, and eventually database integration.

Read these files before implementation:

1. `ARCHITECTURE.md` — target system and design constraints.
2. `LEARNING_PLAN.md` — incremental labs and experiments.
3. `AGENTS.md` — strict instructions for Codex/other coding agents.

## Business story

A customer creates an order. Independent services eventually process payment and inventory, Kafka Streams correlates outcomes, and successful orders progress toward fulfillment.

The domain is intentionally thin. Kafka is the main subject.

## Technology baseline

```text
Java:        21
Spring Boot: 4.1.1
Kafka:       4.3.1
Build:       Maven
Runtime:     Docker Compose
Kafka mode:  KRaft
Kafka UI:    Kafbat UI 1.5.0
Database:    none initially
```

## Learning strategy

Do not generate the final architecture in one pass.

The repository grows lab by lab:

```text
broker basics
    -> producer/consumer
    -> keys + partitions + ordering
    -> consumer groups + rebalance
    -> offsets + replay
    -> failures + retries + DLT
    -> duplicates + idempotency
    -> Kafka transactions
    -> Kafka Streams
    -> compaction + retention
    -> schemas
    -> multi-broker replication
    -> observability
    -> DB + transactional outbox
```

## First Agent task

Give your coding agent the repository containing these four files and use this prompt:

```text
Read AGENTS.md, ARCHITECTURE.md, LEARNING_PLAN.md, and README.md completely.

Implement Lab 00 and Lab 01 only.
Do not implement any later lab or add infrastructure for future labs.

Requirements:
- Java 21.
- Maven multi-module project.
- Spring Boot 4.1.1.
- Apache Kafka 4.3.1 using the official apache/kafka Docker image in KRaft mode.
- Kafbat UI v1.5.0.
- common-events module.
- order-service with POST /orders.
- order-service publishes OrderCreated JSON records to order.created using orderId as the Kafka key.
- payment-service consumes order.created using group ID payment-service and only logs/processes the event; it must not produce payment events yet.
- order.created must have exactly 3 partitions and replication factor 1.
- consumed-record logs must expose key, topic, partition, offset, group/service instance.
- no database.
- no retries/DLT.
- no manual acknowledgement lab yet.
- no Kafka transactions.
- no Kafka Streams.
- no schema registry.
- no inventory/fulfillment/notification services.

Package order-service and payment-service into Docker images and include them in Docker Compose.
Provide health endpoints if needed for Compose readiness.

Add exact README commands for:
1. building the project;
2. starting Docker Compose;
3. submitting an order with curl;
4. listing/describing the order.created topic;
5. inspecting the payment-service consumer group;
6. stopping payment-service, producing orders, restarting it, and observing catch-up;
7. opening Kafbat UI.

Run mvn verify and validate the Docker Compose configuration.
Stop after Lab 01 and summarize the hands-on experiments I should perform before continuing.
```

## Expected Lab 01 API shape

The exact Java implementation is left to the Agent, but keep the API simple. For example:

```http
POST /orders
Content-Type: application/json
```

```json
{
  "customerId": "customer-123",
  "items": [
    {
      "productId": "coffee-machine",
      "quantity": 1,
      "unitPrice": 129.99
    }
  ],
  "currency": "EUR"
}
```

A useful response is simply the generated `orderId` and acceptance status. There is no DB-backed order query yet.

## What to inspect after Lab 01

Do not immediately ask the Agent for Lab 02. First answer these questions using the running system:

- Which partition did each order enter?
- What is the record key?
- Are offsets global or partition-specific?
- What identifies the payment consumer group?
- What happens to records while `payment-service` is stopped?
- How does the restarted consumer know where to continue?
- What changes if you create a brand-new consumer group?

If these are visible and understandable, proceed to Lab 02.

## Lab 00 and Lab 01: run the project

Prerequisites: Java 21, Maven, Docker Desktop, and Docker Compose.

Build the Maven multi-module project first. The Dockerfiles use the packaged
application JARs from this command.

```bash
mvn verify
```

Start Kafka, Kafbat UI, and the two services:

```bash
docker compose up --build -d
docker compose ps
```

Open Kafbat UI at <http://localhost:8081>. The `order.created` topic is
created explicitly by `order-service` with three partitions and replication
factor one.

Create an order:

```bash
curl --request POST http://localhost:8080/orders \
  --header 'Content-Type: application/json' \
  --data '{
    "customerId": "customer-123",
    "items": [
      {"productId": "coffee-machine", "quantity": 1, "unitPrice": 129.99}
    ],
    "currency": "EUR"
  }'
```

Watch the producer and consumer logs. The payment consumer prints the record
key, topic, partition, offset, consumer group, and service instance.

```bash
docker compose logs --follow order-service payment-service
```

List and describe the topic from the Kafka CLI inside the broker container:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --list

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --describe --topic order.created
```

Inspect the `payment-service` group and its committed offsets:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 --describe --group payment-service
```

For the catch-up experiment, stop the consumer, submit one or more orders with
the curl command above, then restart it and follow its logs:

```bash
docker compose stop payment-service
# Run the curl command above one or more times while payment-service is stopped.
docker compose start payment-service
docker compose logs --follow payment-service
```

To repeat the Lab 00 CLI exercise with an isolated temporary topic:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --create --topic lab00.temp --partitions 1 --replication-factor 1

docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:9092 --topic lab00.temp <<'EOF'
first record
second record
EOF

docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic lab00.temp --from-beginning --max-messages 2
```

Stop the environment when finished:

```bash
docker compose down
```

### Hands-on experiments before Lab 02

1. Submit one order and find its key, partition, and offset in Kafbat UI and
   the payment-service log.
2. Submit ten orders and compare their keys, partitions, and per-partition
   offsets. Offsets are partition-specific, not global to the topic.
3. Stop `payment-service`, create orders, restart it, and observe it catch up.
4. Restart `payment-service` a second time and confirm normally committed
   records are not reprocessed.
5. Inspect `payment-service` with `kafka-consumer-groups.sh`, then create a
   separate consumer group manually if you want to compare its independent
   starting position.

