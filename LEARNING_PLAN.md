# Kafka Learning Plan

## How to use this plan

Work through one lab at a time. The Agent must implement only the active lab. After implementation, run the experiments manually and inspect Kafka before moving forward.

Each lab should end with a working Git commit so changes remain easy to compare.

---

## Lab 00 — Infrastructure and visibility

### Goal
Understand what is actually running before writing Kafka application code.

### Implement
- Maven parent project.
- Docker Compose with one Apache Kafka 4.3.1 broker in KRaft mode.
- Kafbat UI.
- Basic README commands.
- No Spring services yet if doing this lab separately.

### Experiments
- Start Kafka.
- List topics using Kafka CLI.
- Create a temporary topic manually.
- Produce several string records with Kafka CLI.
- Consume them with Kafka CLI.
- Inspect topic, partitions, offsets, and records in Kafbat UI.

### Learn
Broker, controller/KRaft, topic, partition, offset, producer, consumer.

---

## Lab 01 — First Spring producer and consumer

### Goal
Build the smallest useful Spring Boot Kafka flow.

### Implement
- `common-events` with `OrderCreated` contract.
- `order-service` exposing `POST /orders`.
- `order-service` publishes `OrderCreated` to `order.created`.
- Kafka record key must be `orderId`.
- `payment-service` consumes `order.created` using `@KafkaListener`.
- Payment logic only logs the received order for now.
- Explicit `order.created` topic with 3 partitions.
- Both services run in Docker Compose.

### Experiments
1. Submit one order and inspect its Kafka record.
2. Submit 10 orders and inspect keys, partitions, and offsets.
3. Stop `payment-service`, create orders, restart it, and observe consumption.
4. Restart the consumer again and verify already committed records are not normally reprocessed.

### Inspect in logs
For every consumed record print:

```text
key, topic, partition, offset, consumer group
```

### Learn
`KafkaTemplate`, serializer/deserializer, `@KafkaListener`, keys, partitions, offsets, group ID, committed offsets.

### Do not implement yet
Retries, DLT, manual acknowledgement, transactions, DB, inventory, Kafka Streams, schema registry.

---

## Lab 02 — Keys, partitions, and ordering

### Goal
See Kafka ordering guarantees rather than merely read about them.

### Implement
- Endpoint/script capable of generating many events.
- Allow multiple events for the same `orderId` for the experiment.
- Make partition visible in producer callback/logging and consumer logs.

### Experiments
- Send 20 records with the same key: observe one partition and increasing offsets.
- Send records with many keys: observe distribution.
- Send records without a key in a controlled experiment and compare behavior.
- Compare ordering within one partition vs global topic ordering.

### Learn
Key hashing, partition selection, per-partition ordering, why Kafka does not provide global topic ordering across partitions.

---

## Lab 03 — Consumer groups, concurrency, and rebalancing

### Goal
Understand Kafka's parallelism model.

### Implement
- Configure payment consumer concurrency through configuration/environment.
- Make instance ID visible in logs.

### Experiments
With a 3-partition topic:

- 1 consumer instance;
- 2 instances in the same group;
- 3 instances;
- 4 instances and observe an idle consumer;
- kill one instance while processing and observe rebalance;
- start another consumer with a different group ID and see that both groups receive the records independently.

Later repeat with 6 partitions.

### Learn
Consumer group, group coordinator, partition assignment, concurrency ceiling, rebalance, independent subscriptions via different groups.

---

## Lab 04 — Offsets and acknowledgement modes

### Goal
Understand when Kafka considers work consumed.

### Implement
Run controlled variants rather than hiding configuration:

- normal Spring listener acknowledgement behavior;
- manual acknowledgement;
- intentional processing delay/failure.

### Experiments
- Observe committed vs current offsets.
- Kill the process before acknowledgement/commit.
- Restart and observe redelivery.
- Reset a consumer group's offsets using Kafka tooling and replay old records.
- Compare `earliest` and `latest` for a brand-new group.

### Learn
Consumer position, committed offset, poll loop, acknowledgements, redelivery, `auto.offset.reset`, replay.

---

## Lab 05 — Failure handling and blocking retries

### Goal
Understand what happens when a listener throws.

### Implement
- Deterministic failure trigger, e.g. a special customer/product value.
- `DefaultErrorHandler` with an explicit bounded backoff.
- Log delivery attempts.

### Experiments
- Produce a poison record.
- Observe repeated delivery.
- Observe impact on records behind it in the affected partition.
- Compare behavior of other partitions.

### Learn
Listener exceptions, blocking retry, backoff, poison messages, partition progress.

---

## Lab 06 — Non-blocking retry topics and DLT

### Goal
Learn the retry-topic pattern and its trade-offs.

### Implement
- Non-blocking retries for an appropriate consumer.
- Explicit retry/DLT behavior.
- DLT consumer that logs the failed event and metadata.

### Experiments
- Trigger temporary and permanent failures.
- Follow the record through original/retry/DLT topics in Kafbat UI.
- Observe that moving records to retry topics changes strict ordering characteristics.

### Important constraint
Spring Kafka non-blocking retry topics are not combined with container transactions. Treat this as a design choice, not a feature to pile on top of the transactions lab.

### Learn
Retry topics, backoff, DLT, failure metadata, ordering trade-offs.

---

## Lab 07 — Duplicate delivery and idempotency

### Goal
Accept that at-least-once delivery can create duplicate processing and design for it.

### Implement
- Add `eventId`-based duplicate detection using an intentionally simple in-memory store first.
- Add a visible side effect counter to demonstrate duplicate effects.

### Experiments
- Force redelivery of an already processed record.
- Reset offsets and replay.
- Compare non-idempotent and idempotent handlers.
- Restart the service and observe why an in-memory idempotency store is insufficient in production.

### Learn
At-least-once delivery, duplicate effects, idempotent consumers, distinction between Kafka delivery and business side effects.

---

## Lab 08 — Producer reliability and Kafka transactions

### Goal
Understand producer durability/idempotence and transactional read-process-write.

### Implement
- `payment-service` now emits `payment.completed` or `payment.failed`.
- Study/configure producer acknowledgements and idempotence.
- Add a transactional read-process-write variant where the consumed offset and produced Kafka record participate in one Kafka transaction.

### Experiments
- Fail after consuming but before producing.
- Fail after producing but before completion.
- Compare non-transactional and transactional behavior.
- Inspect consumers configured with appropriate isolation behavior.

### Learn
`acks`, idempotent producer, producer IDs/sequence semantics conceptually, Kafka transactions, `read_committed`, exactly-once semantics for Kafka read-process-write.

---

## Lab 09 — Kafka Streams and state

### Goal
Learn stream processing as a different programming model from listeners.

### Implement
- Add `inventory-service` producing reservation outcomes.
- Add `fulfillment-service` using Kafka Streams.
- Correlate successful payment and inventory reservation by `orderId`.
- Emit `order.ready-for-shipment` when both conditions are met.

### Experiments
- Send payment first vs inventory first.
- Restart the Streams application.
- Inspect internal/changelog topics and local state where practical.
- Run multiple Streams instances and observe task distribution.

### Learn
KStream, KTable/state store as appropriate, joins, topology, tasks, changelog/repartition topics, recovery.

---

## Lab 10 — Compaction, retention, and replay

### Goal
Understand that Kafka can represent both event history and latest state.

### Implement
- Add `order.state` keyed by `orderId` with log compaction.
- Keep an event-history topic with normal retention.

### Experiments
- Publish several states for one order.
- Compare history topic vs compacted state topic.
- Introduce tombstones deliberately.
- Rebuild a simple projection by replaying Kafka records.

### Learn
Retention, log compaction, tombstones, event log vs latest-state log, replay/projection rebuilding.

---

## Lab 11 — Serialization and schema evolution

### Goal
Understand event contracts and compatibility.

### Implement in two steps
1. Evolve the existing JSON event and demonstrate compatibility problems/solutions.
2. Introduce Schema Registry plus either Avro or Protobuf as a separate change.

### Experiments
- Old consumer with new producer.
- New consumer with old records.
- Compatible field addition.
- Deliberately incompatible change.

### Learn
Serializer/deserializer, schema evolution, compatibility, why schemas matter in asynchronous systems.

---

## Lab 12 — Multi-broker durability

### Goal
Move beyond the single-broker development setup.

### Implement
- 3 Kafka brokers/controllers in a suitable local KRaft topology.
- Topics with replication factor > 1.
- Explicit durability-related settings for experiments.

### Experiments
- Inspect leaders and replicas.
- Stop a broker hosting a partition leader.
- Observe leader election and continued availability where replication permits it.
- Inspect ISR changes.

### Learn
Replication factor, leader/follower replicas, ISR, broker failure, `acks=all`, durability trade-offs.

---

## Lab 13 — Observability

### Goal
Diagnose Kafka behavior using metrics rather than only logs.

### Implement
- Spring Boot Actuator/Micrometer metrics.
- Useful consumer/producer metrics.
- Optional Prometheus/Grafana only here, not earlier.

### Experiments
- Artificially slow a consumer and observe lag.
- Increase consumer instances and compare lag recovery.
- Trigger errors/retries and inspect metrics.

### Learn
Consumer lag, throughput, latency, error metrics, operational signals.

---

## Lab 14 — Database, dual write, and transactional outbox

### Goal
Introduce a database only when it teaches an important distributed-systems/Kafka problem.

### Implement
- PostgreSQL.
- Persist an order and publish an event using a deliberately naive dual-write version first.
- Demonstrate inconsistency failure windows.
- Replace it with a transactional outbox.
- Publish outbox records to Kafka using a simple publisher; optionally study CDC/Debezium afterward as an extension.

### Experiments
- Crash between DB commit and Kafka publish.
- Crash around outbox publication.
- Replay/publish safely and inspect duplicates.

### Learn
Dual-write problem, local ACID transaction, outbox, eventual consistency, idempotency.

---

## Optional advanced labs

After the core sequence:

- Kafka Connect + Debezium CDC.
- Security: SASL/TLS and ACLs.
- Consumer static membership/cooperative rebalancing experiments.
- Batch listeners.
- Producer/consumer interceptors.
- Header propagation and tracing.
- Performance/load experiments.
- Topic configuration tuning.
- Testcontainers-based integration tests.

These are extensions, not prerequisites for understanding the core model.
