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

Both services also write to files on the host, which persist while the Compose
environment is running:

```bash
tail -f logs/order-service/order-service.log \
  logs/payment-service/payment-service.log
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

## Lab 02 — Keys, partitions, and ordering

Lab 02 adds one deliberately small experiment endpoint:

```text
POST /orders/experiments
```
It publishes a sequential batch of `OrderCreated` records and returns the key,
topic, partition, and offset for every successful send. `orderId` remains the
Kafka key by default. Set `withoutKey` to `true` only for the controlled
null-key experiment. This endpoint uses fixed, minimal order data because its
purpose is observing Kafka partitioning, not order processing.

Run the normal build and start commands from Lab 01, then watch both services:

```bash
docker compose logs --follow order-service payment-service
```

### 1. One key: one partition and increasing offsets

Publish 20 records with exactly the same key and payload `orderId`:

```bash
curl --request POST http://localhost:8080/orders/experiments \
  --header 'Content-Type: application/json' \
  --data '{
    "orderId": "lab02-same-key",
    "count": 20,
    "withoutKey": false
  }'
```

The response and order-service logs show the producer metadata. The
payment-service log should show the same key and partition, with increasing
offsets for these records.

### 2. Many keys: observe distribution

Publish one record for each of 20 distinct order IDs:

```bash
for order_number in $(seq 1 20); do
  curl --silent --show-error --request POST http://localhost:8080/orders/experiments \
    --header 'Content-Type: application/json' \
    --data "{\"orderId\":\"lab02-many-${order_number}\",\"count\":1,\"withoutKey\":false}"
done
```

Compare the partitions in the responses and logs. With a small sample, records
need not be distributed evenly across the three partitions.

### 3. Null-key comparison

Publish a separate batch whose Kafka key is `null` while the event payload
continues to contain an `orderId`:

```bash
curl --request POST http://localhost:8080/orders/experiments \
  --header 'Content-Type: application/json' \
  --data '{
    "orderId": "lab02-no-key",
    "count": 20,
    "withoutKey": true
  }'
```

This is intentionally not the normal order flow. A null key has no
deterministic key-to-partition mapping; the Kafka producer's partitioner makes
the selection.

### Inspect the topic and consumer group

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --describe --topic order.created

docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 --describe --group payment-service
```

In Kafbat UI at <http://localhost:8081>, inspect `order.created` by partition
rather than treating the topic as one globally ordered stream.

### What to inspect after Lab 02

- Do the 20 `lab02-same-key` records all have the same partition?
- Are their offsets strictly increasing within that one partition?
- Do the `lab02-many-*` keys map across the available partitions? Why is a
  perfectly even spread not expected from a small sample?
- With `withoutKey: true`, is a record deterministically tied to one partition?
- Are offsets global to the topic, or specific to each partition?
- Does the order in one partition establish any relative order with records in
  another partition?

Kafka guarantees record order within a partition. A multi-partition topic has
no single global record order.

## Lab 03 — Consumer groups, concurrency, and rebalancing

`payment-service` remains in the `payment-service` consumer group. Its listener
concurrency is now explicit and configurable through
`PAYMENT_LISTENER_CONCURRENCY`, which defaults to `1`. Each consumer log also
includes its runtime `serviceInstance` and `listenerThread` so a container and
its listener threads can be distinguished.

After rebuilding with `mvn verify`, use these commands to recreate only the
payment consumers. The `order.created` topic still has three partitions, so no
consumer group can process more than three partitions in parallel.

### One service instance

```bash
PAYMENT_LISTENER_CONCURRENCY=1 \
  docker compose up --build --detach --force-recreate --scale payment-service=1 payment-service

docker compose logs --follow payment-service
```

Publish a batch of distinct keys using the Lab 02 many-key command, then inspect
the group assignment:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 --describe --group payment-service
```

One consumer instance owns all three partitions.

### Two, three, and four service instances in the same group

Keep listener concurrency at one per instance, then scale the service. Repeat
the many-key batch after each command and compare the logs and group assignment.

```bash
PAYMENT_LISTENER_CONCURRENCY=1 \
  docker compose up --detach --force-recreate --scale payment-service=2 payment-service

PAYMENT_LISTENER_CONCURRENCY=1 \
  docker compose up --detach --force-recreate --scale payment-service=3 payment-service

PAYMENT_LISTENER_CONCURRENCY=1 \
  docker compose up --detach --force-recreate --scale payment-service=4 payment-service
```

With three partitions, at most three consumers in the group receive partition
assignments. With four service instances, one is idle. Inspect members and their
assignments explicitly:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 --describe --group payment-service --members --verbose
```

### Listener concurrency within one service instance

This is a separate way to create consumers in the same group. Run one container
with three listener threads, publish distinct keys, and compare
`listenerThread` values in its logs:

```bash
PAYMENT_LISTENER_CONCURRENCY=3 \
  docker compose up --detach --force-recreate --scale payment-service=1 payment-service

docker compose logs --follow payment-service
```

The concurrency ceiling is still the three topic partitions. Increasing it to
four does not make a fourth partition available.

### Rebalance experiment

Start three payment-service instances, publish a larger many-key batch, then
identify one container and kill it while records are being consumed:

```bash
docker compose ps payment-service
docker kill <one-payment-service-container-id>

docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 --describe --group payment-service --members --verbose
```

The remaining members receive new partition assignments after the rebalance.
Start the missing instance again with the three-instance scale command above and
observe another rebalance.

### Independent consumer group

In a second terminal, start one temporary consumer with a different group ID:

```bash
docker compose run --rm \
  --name payment-service-observer \
  -e SPRING_KAFKA_CONSUMER_GROUP_ID=payment-service-observer \
  -e SERVICE_INSTANCE_ID=payment-service-observer \
  payment-service
```

Publish another batch while this command is running. Both `payment-service` and
`payment-service-observer` receive the records because each group maintains its
own partition assignments and offsets. Stop the temporary observer with
`Ctrl-C` when finished.

### What to inspect after Lab 03

- Does each partition have at most one assigned consumer within one group?
- How do assignments change from one to two and then three instances?
- Why is one of four instances idle for a three-partition topic?
- How do `serviceInstance` and `listenerThread` differ when scaling containers
  versus increasing listener concurrency?
- What happens to partition ownership immediately after an instance is killed?
- Why does a consumer in `payment-service-observer` receive records already
  consumed by the `payment-service` group?

## Lab 04 — Offsets and acknowledgement modes

Lab 04 makes the payment listener's acknowledgement behavior explicit while
keeping the default path close to Spring Kafka's normal batch acknowledgement.
There are no custom retries, DLTs, transactions, or idempotency logic in this
lab.

| Environment variable | Default | Purpose |
| --- | --- | --- |
| `PAYMENT_ACK_MODE` | `BATCH` | Spring Kafka acknowledgement mode; use `MANUAL_IMMEDIATE` for the manual experiment. |
| `PAYMENT_PROCESSING_DELAY_MS` | `0` | Delays processing before a manual acknowledgement, creating time to stop the consumer. |
| `PAYMENT_FAILURE_ORDER_ID` | `disabled` | Throws before acknowledgement only when the payload order ID exactly matches this value. |

Before each experiment, use a single payment-service instance so group changes
do not obscure acknowledgement behavior:

```bash
PAYMENT_LISTENER_CONCURRENCY=1 \
  docker compose up --build --detach --force-recreate --scale payment-service=1 payment-service
```

### Observe normal batch acknowledgement

Use the default `BATCH` mode, produce a Lab 02 batch, and compare the
consumer's current and committed offsets after it processes the records:

```bash
PAYMENT_ACK_MODE=BATCH \
  docker compose up --detach --force-recreate --scale payment-service=1 payment-service

docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 --describe --group payment-service
```

In this mode the listener does not call `Acknowledgment` itself. The container
commits eligible processed records when it completes the poll batch.

### Observe manual acknowledgement and redelivery

Run with manual-immediate acknowledgement and a 30-second delay. The manual
acknowledgement happens only after that delay:

```bash
PAYMENT_ACK_MODE=MANUAL_IMMEDIATE PAYMENT_PROCESSING_DELAY_MS=30000 \
  docker compose up --detach --force-recreate --scale payment-service=1 payment-service

docker compose logs --follow payment-service
```

While the log shows `Delaying processing`, publish one record with a distinctive
order ID in another terminal:


```bash
curl --request POST http://localhost:8080/orders/experiments \
  --header 'Content-Type: application/json' \
  --data '{"orderId":"lab04-redelivery","count":1,"withoutKey":false}'
```

Kill the payment-service container before it prints `Manually acknowledged`,
then start it again with the same manual-mode command. The uncommitted record
is delivered again:

```bash
docker compose ps payment-service
docker kill <payment-service-container-id>
```

### Controlled listener failure before acknowledgement

The following optional experiment makes one matching record throw before its
manual acknowledgement. It deliberately configures no error handler; bounded
retry behavior is introduced only in Lab 05.

```bash
PAYMENT_ACK_MODE=MANUAL_IMMEDIATE PAYMENT_FAILURE_ORDER_ID=lab04-failure \
  docker compose up --detach --force-recreate --scale payment-service=1 payment-service

curl --request POST http://localhost:8080/orders/experiments \
  --header 'Content-Type: application/json' \
  --data '{"orderId":"lab04-failure","count":1,"withoutKey":false}'
```

Follow the payment-service logs and inspect the group offsets. Restore the
default `PAYMENT_FAILURE_ORDER_ID=disabled` before further experiments.

### Reset and replay a group

Kafka refuses to reset offsets for an active group, so stop the payment service
first. Reset the group's `order.created` offsets to the beginning, then start
the normal BATCH-mode consumer and observe the replay:

```bash
docker compose stop payment-service

docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --group payment-service \
  --topic order.created \
  --reset-offsets --to-earliest --execute

PAYMENT_ACK_MODE=BATCH \
  docker compose up --detach --scale payment-service=1 payment-service
```

### `earliest` versus `latest` for a new group

Start the following command in a separate terminal before producing records.
The `lab04-latest` group starts at the end of each partition and therefore sees
only records produced after it begins:

```bash
docker compose run --rm \
  --name payment-service-latest \
  -e SPRING_KAFKA_CONSUMER_GROUP_ID=lab04-latest \
  -e SPRING_KAFKA_CONSUMER_AUTO_OFFSET_RESET=latest \
  -e SERVICE_INSTANCE_ID=payment-service-latest \
  payment-service
```

Repeat with `lab04-earliest` and `SPRING_KAFKA_CONSUMER_AUTO_OFFSET_RESET=earliest`
to consume the existing history. Stop each temporary consumer with `Ctrl-C`.

### What to inspect after Lab 04

- For each partition, how do the group's current and committed offsets change
  after normal BATCH processing?
- Why does killing the manual-mode consumer during its delay cause the record
  to reappear after restart?
- Why must a group be inactive before its offsets are reset?
- What does replay prove about the difference between a Kafka record and a
  completed business side effect?
- How does `auto.offset.reset` affect a group with no committed offsets, and
  why does it not override existing committed offsets?

Do not add bounded retries or a DLT yet; those belong to Lab 05 and Lab 06.

Do not proceed to offset acknowledgement or replay behavior yet; those are Lab
04 topics.

