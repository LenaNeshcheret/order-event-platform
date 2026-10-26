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
docker kill "$(docker compose ps -q payment-service | head -n 1)"

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

### Goal

See exactly when a consumer group advances its committed offset. Kafka stores the **next** offset to read: committed offset `3` means records `0`, `1`, and `2` are complete, and restart begins at `3`.

### Implementation used by this lab

The service uses two listener variants, selected by `PAYMENT_LISTENER_STYLE`. Exactly one starts.

| Listener style | Valid acknowledgement modes | Listener contract |
| --- | --- | --- |
| `automatic` | `BATCH`, `RECORD`, `TIME`, `COUNT`, `COUNT_TIME` | Receives a `ConsumerRecord`; Spring commits after success according to the selected mode. |
| `manual` | `MANUAL`, `MANUAL_IMMEDIATE` | Receives `ConsumerRecord` and `Acknowledgment`; it calls `acknowledge()` only after success. |

The application fails at startup for an invalid style/mode combination. Manual acknowledgement is a different listener contract, not a toggle an automatic listener can safely ignore.

There is no custom error handler, retry topic, DLT, transaction, or idempotency logic in this lab. Retry and recovery policy belongs to Labs 05 and 06.

| Environment variable | Default | Purpose |
| --- | --- | --- |
| `PAYMENT_LISTENER_STYLE` | `automatic` | Selects the automatic or manual listener variant. |
| `PAYMENT_ACK_MODE` | `BATCH` | Spring Kafka acknowledgement mode. |
| `PAYMENT_CONSUMER_GROUP_ID` | `payment-service7` | Group inspected by this experiment. |
| `PAYMENT_AUTO_OFFSET_RESET` | `earliest` | Used only when this group has no usable committed offset. |
| `PAYMENT_MAX_POLL_RECORDS` | `500` | Set to `5` for the five-record manual comparison. |
| `PAYMENT_PROCESSING_DELAY_MS` | `0` | Delay before acknowledgement; creates an observable crash window. |
| `PAYMENT_FAILURE_OFFSET` | `-1` | Throws before acknowledgement only for this Kafka offset; `-1` disables it. |
| `PAYMENT_FAILURE_ORDER_ID` | `disabled` | Alternative failure trigger based on the payload order ID. |

### Modes to learn

| Mode | Commit point after successful processing |
| --- | --- |
| `BATCH` | After every record returned by the current `poll()` completes. |
| `RECORD` | After each listener invocation returns. |
| `MANUAL` | The listener calls `acknowledge()`, then Spring commits using `BATCH` semantics. |
| `MANUAL_IMMEDIATE` | Immediately when the listener calls `acknowledge()` on its consumer thread. |
| `TIME`, `COUNT`, `COUNT_TIME` | Automatic modes that add a time threshold, record-count threshold, or either threshold. Read about them now; defer hands-on experiments until the four modes above are clear. |

`Acknowledgment submitted` in the log does **not** necessarily mean the offset is committed: with `MANUAL`, it is queued until the poll completes. The group description is the source of truth.

### Step 1 — Build and isolate one consumer

```bash
mvn verify
docker compose up --build --detach kafka kafbat-ui order-service
docker compose stop payment-service
```

Keep `PAYMENT_LISTENER_CONCURRENCY=1` throughout Lab 04. Multiple instances and partition assignment are Lab 03 topics.
`kafbat-ui` is the Compose service for the browser UI. `kafka-consumer-groups.sh`
is a Kafka CLI program, executed inside the `kafka` container with
`docker compose exec kafka ...`; it is not a Compose service.

### Step 2 — Prepare five records in one partition

Before each comparison, set `GROUP` to the group name used by that comparison.
For example, use `lab04-batch` for Step 3. The group must be inactive before
Kafka permits a reset.

```bash
GROUP=lab04-batch

docker compose stop payment-service

docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --group "$GROUP" \
  --topic order.created \
  --reset-offsets --to-latest --execute
```

If a brand-new group does not yet exist, start payment-service once with that group and `PAYMENT_AUTO_OFFSET_RESET=latest`, wait for `partitions assigned`, stop it, then repeat the reset command.

Publish exactly five records while payment-service is stopped. One key keeps all five in one partition. In the response, call the first offset `S`; the fourth record is `S + 3`.

```bash
curl --request POST http://localhost:8080/orders/experiments \
  --header 'Content-Type: application/json' \
  --data '{"orderId":"lab04-five","count":5,"withoutKey":false}'
```

### Step 3 — Compare automatic `BATCH` and `RECORD`

Prepare five records using Step 2 with group `lab04-batch`, then run. This
sets Kafka's `max.poll.records=2` through the Docker Compose environment
variable `PAYMENT_MAX_POLL_RECORDS=2`:

```bash
PAYMENT_CONSUMER_GROUP_ID=lab04-batch \
PAYMENT_AUTO_OFFSET_RESET=latest \
PAYMENT_LISTENER_STYLE=automatic \
PAYMENT_ACK_MODE=BATCH \
PAYMENT_LISTENER_CONCURRENCY=1 \
PAYMENT_MAX_POLL_RECORDS=2 \
  docker compose up --build --detach --force-recreate payment-service

docker compose logs --follow payment-service
```

After all five records succeed, inspect the committed offsets:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 --describe --group lab04-batch
```

Repeat with group `lab04-record` and `PAYMENT_ACK_MODE=RECORD`, keeping
`PAYMENT_MAX_POLL_RECORDS=2`. Both modes commit five successful records.
`RECORD` can commit after each listener return; `BATCH` waits for the
complete poll. The next two steps make that boundary visible.

### Step 4 — `MANUAL_IMMEDIATE`: commit the first three records

Prepare five records using Step 2 with group `lab04-manual-immediate`. Set `PAYMENT_MAX_POLL_RECORDS=5` so the five records can be returned by one poll.

```bash
PAYMENT_CONSUMER_GROUP_ID=lab04-manual-immediate \
PAYMENT_AUTO_OFFSET_RESET=latest \
PAYMENT_LISTENER_STYLE=manual \
PAYMENT_ACK_MODE=MANUAL_IMMEDIATE \
PAYMENT_MAX_POLL_RECORDS=5 \
PAYMENT_PROCESSING_DELAY_MS=10000 \
  docker compose up --build --detach --force-recreate payment-service

docker compose logs --follow payment-service
```

Wait until `S`, `S + 1`, and `S + 2` log `Acknowledgment submitted`. When `S + 3` logs `Delaying processing`, kill the container before its delay ends:

```bash
docker compose kill payment-service
```

Inspect the group. Its committed offset should be `S + 3`, which is the next record to read; the first three are not replayed. Restart with the same group and mode, but `PAYMENT_PROCESSING_DELAY_MS=0`. The first consumed record on that partition should be `S + 3`.

```bash
PAYMENT_CONSUMER_GROUP_ID=lab04-manual-immediate \
PAYMENT_AUTO_OFFSET_RESET=latest \
PAYMENT_LISTENER_STYLE=manual \
PAYMENT_ACK_MODE=MANUAL_IMMEDIATE \
PAYMENT_MAX_POLL_RECORDS=5 \
PAYMENT_PROCESSING_DELAY_MS=0 \
  docker compose up --build --detach --force-recreate payment-service
```

### Step 5 — `MANUAL`: queue the first three acknowledgements

Repeat Step 4 with a new group named `lab04-manual`, but set `PAYMENT_ACK_MODE=MANUAL`.

```bash
PAYMENT_CONSUMER_GROUP_ID=lab04-manual \
PAYMENT_AUTO_OFFSET_RESET=latest \
PAYMENT_LISTENER_STYLE=manual \
PAYMENT_ACK_MODE=MANUAL \
PAYMENT_MAX_POLL_RECORDS=5 \
PAYMENT_PROCESSING_DELAY_MS=10000 \
  docker compose up --build --detach --force-recreate payment-service
```

Kill the container while `S + 3` is delayed. The first three acknowledgements were submitted, but `MANUAL` uses batch semantics and the five-record poll did not complete. The committed offset should remain `S`.

Restart with delay `0`. The partition starts at `S`, so `S`, `S + 1`, and `S + 2` are delivered again. This intentional duplicate delivery is why later labs introduce idempotency.

### Step 6 — Optional: throw on the fourth record

Repeat the manual-immediate setup, replacing the delay-only crash window with the integer offset of the fourth record. Replace `123` below with `S + 3` from the producer response:

```bash
PAYMENT_CONSUMER_GROUP_ID=lab04-manual-immediate \
PAYMENT_AUTO_OFFSET_RESET=latest \
PAYMENT_LISTENER_STYLE=manual \
PAYMENT_ACK_MODE=MANUAL_IMMEDIATE \
PAYMENT_MAX_POLL_RECORDS=5 \
PAYMENT_PROCESSING_DELAY_MS=0 \
PAYMENT_FAILURE_OFFSET=123 \
  docker compose up --build --detach --force-recreate payment-service
```

The processor throws before acknowledgement at that offset. Kill the container after the first failure, before Spring's default error handling can recover it. Restart with `PAYMENT_FAILURE_OFFSET=-1`. This proves that an exception before acknowledgement leaves the record uncommitted; Lab 05 studies retry and recovery policy.

### Step 7 — `earliest` versus `latest` for a new group

Run two short automatic `BATCH` experiments with otherwise identical settings:

- group `lab04-earliest` with `PAYMENT_AUTO_OFFSET_RESET=earliest` reads retained history;
- group `lab04-latest` with `PAYMENT_AUTO_OFFSET_RESET=latest` starts at the end and sees only later records.

`auto-offset-reset` applies only when the group has no usable committed offset. It never overrides an existing committed position.

### Questions to answer before Lab 05

1. Why does a committed offset of `S + 3` mean that records through `S + 2` are complete?
2. Why does `MANUAL_IMMEDIATE` restart at `S + 3`, while `MANUAL` restarts at `S` in the incomplete five-record-poll experiment?
3. Why is `Acknowledgment submitted` not proof of a durable commit in `MANUAL` mode?
4. What happens if a business side effect completes but the process dies before the offset commits?
5. Why must the group be inactive before Kafka resets its offsets?
6. Why must all five experiment records use one key and therefore one partition?
7. When does `auto-offset-reset` apply, and why does changing it not replay an already-committed group?
8. Why are `TIME`, `COUNT`, and `COUNT_TIME` less useful than the four core modes for the first acknowledgement experiment?

Do not add bounded retries, DLTs, transactions, or idempotency yet; those belong to later labs.

## Lab 05 — Failure handling and blocking retries

### Goal

Observe what happens when a listener throws. This lab deliberately uses a
blocking retry: the failed partition is paused while the same record is retried.
Records behind it in that partition cannot progress until it succeeds or the
configured retries are exhausted. Other partitions can still make progress.

### Implementation used by this lab

`PAYMENT_FAILURE_ORDER_ID` is the deterministic poison-record trigger. When
the payload order ID matches its value, `payment-service` throws before the
listener returns. A `DefaultErrorHandler` retries the same record with an
explicit `FixedBackOff`; it logs each failed delivery with the delivery attempt,
record key, topic, partition, and offset.

| Environment variable | Default | Purpose |
| --- | --- | --- |
| `PAYMENT_FAILURE_ORDER_ID` | `disabled` | Exact order ID that should fail on every delivery. |
| `PAYMENT_RETRY_BACKOFF_MS` | `1000` | Delay between retries of the failed record. |
| `PAYMENT_RETRY_MAX_RETRIES` | `2` | Retries after the initial delivery; the poison record is delivered three times in total. |

After the two retries are exhausted, the configured recoverer logs that the
poison record is skipped and allows the partition to continue. It does **not**
publish to a DLT or retry topic. Those are Lab 06 topics. This is an
at-least-once listener: do not add side effects to the processor yet.

### Step 1 — Start one automatic consumer

Use a fresh group and automatic `BATCH` acknowledgement so the failure handler
is the only behavior under study:

```bash
mvn verify
docker compose up --build --detach kafka kafbat-ui order-service

PAYMENT_CONSUMER_GROUP_ID=lab05-blocking \
PAYMENT_AUTO_OFFSET_RESET=latest \
PAYMENT_LISTENER_STYLE=automatic \
PAYMENT_ACK_MODE=BATCH \
PAYMENT_LISTENER_CONCURRENCY=1 \
PAYMENT_RETRY_BACKOFF_MS=1000 \
PAYMENT_RETRY_MAX_RETRIES=2 \
PAYMENT_FAILURE_ORDER_ID=lab05-poison \
  docker compose up --build --detach --force-recreate payment-service

docker compose logs --follow payment-service
```

### Step 2 — Produce a poison record followed by a healthy record

Both records use the same key, so they go to one partition in order. Submit the
poison record first, then the healthy one:

```bash
curl --request POST http://localhost:8080/orders/experiments \
  --header 'Content-Type: application/json' \
  --data '{"orderId":"lab05-poison","count":1,"withoutKey":false}'

curl --request POST http://localhost:8080/orders/experiments \
  --header 'Content-Type: application/json' \
  --data '{"orderId":"lab05-after-poison","count":1,"withoutKey":false}'
```

The log shows three `Listener failure` entries for the poison record with
increasing `deliveryAttempt` values, approximately one second apart. The
healthy record in that partition is processed only after `Retries exhausted;
skipping poison record` appears.

### Step 3 — Compare another partition

While the poison record is retrying, submit records with different order IDs.
Inspect the producer response or Kafbat UI to choose one assigned to another
partition. That partition can continue processing while the poison record blocks
its own partition; Kafka ordering is per partition, not global to the topic.

```bash
curl --request POST http://localhost:8080/orders/experiments \
  --header 'Content-Type: application/json' \
  --data '{"orderId":"lab05-other-key","count":3,"withoutKey":false}'
```

### What to inspect before Lab 06

1. Why is the poison record delivered three times rather than two?
2. Why does the healthy record behind it wait, even though it never throws?
3. Which records can progress while that partition is blocked, and why?
4. Why does skipping a poison record after retries avoid a permanently stalled partition but risk losing its business effect?
5. Why is a logged recoverer different from a DLT?

Do not add retry topics or a DLT yet; Lab 06 introduces those patterns and
their ordering trade-offs.

