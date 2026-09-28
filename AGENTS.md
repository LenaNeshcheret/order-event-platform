# AGENTS.md — Instructions for Coding Agents

## Project mission

This is a learning repository for Apache Kafka with Spring Boot. Optimize for transparency and hands-on understanding, not for maximum abstraction or production architecture sophistication.

The business domain is e-commerce order processing, but Kafka is the subject.

## Most important rule

**Implement only the currently requested lab from `LEARNING_PLAN.md`. Do not implement future labs proactively.**

If asked to implement Lab 01, do not add retries, DLT, Kafka Streams, transactions, a database, schema registry, or other future concepts.

The learner must encounter Kafka concepts incrementally.

## Baseline

Unless explicitly changed by a later lab:

- Java 21
- Maven
- Spring Boot 4.1.1
- use Spring Boot dependency management for Spring Kafka/Kafka clients
- Apache Kafka Docker image `apache/kafka:4.3.1`
- Kafka KRaft mode; no ZooKeeper
- Kafbat UI `ghcr.io/kafbat/kafka-ui:v1.5.0`
- JSON serialization initially
- Docker Compose for local runtime
- no database

## Engineering principles

1. Prefer simple code that exposes Kafka concepts.
2. Avoid generic abstractions such as `MessageBus`, `EventPublisher`, or custom messaging frameworks unless a lab explicitly studies abstraction.
3. Using `KafkaTemplate` and `@KafkaListener` directly is desirable in early labs.
4. Keep Kafka configuration visible in `application.yml` and/or focused configuration classes.
5. Add comments only where they explain a Kafka-specific reason or non-obvious trade-off.
6. Do not hide important behavior behind Spring defaults when the current lab is intended to study that behavior.
7. Do not add dependencies unrelated to the active lab.
8. Do not add Lombok merely to reduce a few lines of Java.
9. Prefer Java records for immutable event DTOs where appropriate.
10. Keep business logic deliberately small and deterministic.

## Repository discipline

Before changing code:

1. Read `ARCHITECTURE.md`.
2. Read the requested lab in `LEARNING_PLAN.md`.
3. Inspect existing code/configuration.
4. State which files you plan to change.
5. Implement only that scope.

After changing code:

1. Run unit/integration tests available for the active lab.
2. Run `mvn verify` from the repository root.
3. Validate Docker Compose configuration.
4. Update README commands if runtime behavior changed.
5. Summarize exactly what was implemented and what remains intentionally unimplemented.

## Kafka rules

### Record keys
Use `orderId` as the Kafka record key for order-related events unless the lab intentionally studies another strategy.

### Topic creation
Prefer explicit topic creation. Do not rely accidentally on broker auto-topic creation.

### Consumer groups
Use stable, meaningful group IDs. Instances of the same logical service normally use the same group ID.

### Logging
For consumed Kafka records, make these observable when relevant:

- record key;
- topic;
- partition;
- offset;
- consumer group/service instance.

Never log only "message received" when the lab is about Kafka behavior.

### Failure behavior
Do not silently swallow listener exceptions. Failure handling must correspond to the current lab.

### Exactly-once wording
Be precise. Kafka transactions can provide exactly-once semantics for supported Kafka read-process-write flows. Do not claim that arbitrary external side effects become exactly-once.

### Retries
Do not mix non-blocking retry topics with container transactions. If both concepts are being compared, implement them as separate alternatives/labs and document the trade-off.

## Docker rules

- Pin important image versions; avoid `latest` in committed learning infrastructure.
- Keep the first Kafka setup to one broker.
- Do not add ZooKeeper.
- Services inside Compose connect using the Docker Kafka listener/hostname, not `localhost`.
- Host-side CLI/app connections must use the exposed host listener.
- Add health checks where they materially improve deterministic startup, but do not build a large orchestration framework.

## Event contract rules

- Events are facts and should use past-tense names: `OrderCreated`, `PaymentCompleted`, `InventoryReserved`.
- Include `eventId`, `eventVersion`, `occurredAt`, and `correlationId` in the envelope.
- Keep contracts small.
- Do not turn `common-events` into a shared business-logic library.

## Testing rules

Do not mock Kafka for tests whose purpose is Kafka behavior.

Unit-test pure transformations normally. Use integration testing when a lab needs broker semantics. Manual experiments described in `LEARNING_PLAN.md` are part of the definition of done.

## Definition of done for a lab

A lab is done only when:

- the project builds;
- the relevant services start;
- the documented experiment can be executed;
- logs/UI make the intended Kafka behavior observable;
- README contains exact commands;
- future-lab functionality has not leaked into the implementation.

## Current implementation instruction

When initially handed this repository, implement **Lab 00 and Lab 01 only**, unless the user explicitly requests a different lab.

After Lab 01 is working, stop and report the manual experiments the learner should perform before any further implementation.
