package com.olena.payment;

import com.olena.events.OrderCreated;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

@Component
class OrderCreatedProcessor {
    private static final Logger log = LoggerFactory.getLogger(OrderCreatedProcessor.class);

    private final String groupId;
    private final String serviceInstance;
    private final String acknowledgementMode;
    private final long processingDelayMs;
    private final String failureOrderId;
    private final long failureOffset;
    private final String failureAfterSideEffectOrderId;
    private final int failuresBeforeSuccess;
    private final boolean idempotencyEnabled;
    private final InMemoryProcessedEventStore processedEventStore;
    private final PaymentSideEffectCounter sideEffectCounter;
    private final ConcurrentMap<String, AtomicInteger> failureCounts = new ConcurrentHashMap<>();

    OrderCreatedProcessor(
            @Value("${spring.kafka.consumer.group-id}") String groupId,
            @Value("${SERVICE_INSTANCE_ID:${HOSTNAME:payment-service}}") String serviceInstance,
            @Value("${spring.kafka.listener.ack-mode}") String acknowledgementMode,
            @Value("${app.kafka.listener.processing-delay-ms}") long processingDelayMs,
            @Value("${app.kafka.listener.failure-order-id}") String failureOrderId,
            @Value("${app.kafka.listener.failure-offset}") long failureOffset,
            @Value("${app.kafka.listener.failure-after-side-effect-order-id}") String failureAfterSideEffectOrderId,
            @Value("${app.kafka.listener.failures-before-success}") int failuresBeforeSuccess,
            @Value("${app.kafka.listener.idempotency-enabled}") boolean idempotencyEnabled,
            InMemoryProcessedEventStore processedEventStore,
            PaymentSideEffectCounter sideEffectCounter
    ) {
        this.groupId = groupId;
        this.serviceInstance = serviceInstance;
        this.acknowledgementMode = acknowledgementMode;
        this.processingDelayMs = processingDelayMs;
        this.failureOrderId = failureOrderId;
        this.failureOffset = failureOffset;
        this.failureAfterSideEffectOrderId = failureAfterSideEffectOrderId;
        this.failuresBeforeSuccess = failuresBeforeSuccess;
        this.idempotencyEnabled = idempotencyEnabled;
        this.processedEventStore = processedEventStore;
        this.sideEffectCounter = sideEffectCounter;
    }

    void process(ConsumerRecord<String, OrderCreated> record) {
        log.info("Processing OrderCreated: key={}, topic={}, partition={}, offset={}, consumerGroup={}, serviceInstance={}, listenerThread={}, acknowledgementMode={}, orderId={}",
                record.key(),
                record.topic(),
                record.partition(),
                record.offset(),
                groupId,
                serviceInstance,
                Thread.currentThread().getName(),
                acknowledgementMode,
                record.value().payload().orderId());

        delayProcessing();

        if (record.offset() == failureOffset) {
            throw new IllegalStateException("Intentional failure for offset=" + failureOffset);
        }
        String orderId = record.value().payload().orderId();
        if (orderId.equals(failureOrderId)) {
            int failureAttempt = failureCounts
                    .computeIfAbsent(orderId, ignored -> new AtomicInteger())
                    .incrementAndGet();
            if (failuresBeforeSuccess < 0 || failureAttempt <= failuresBeforeSuccess) {
                throw new IllegalStateException("Intentional failure for orderId=" + orderId
                        + ", failureAttempt=" + failureAttempt);
            }
        }

        if (idempotencyEnabled && !processedEventStore.markProcessedIfFirstDelivery(record.value().eventId())) {
            log.warn("Duplicate OrderCreated ignored: eventId={}, key={}, topic={}, partition={}, offset={}, consumerGroup={}, serviceInstance={}, sideEffectCount={}",
                    record.value().eventId(),
                    record.key(),
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    groupId,
                    serviceInstance,
                    sideEffectCounter.currentCount());
            return;
        }

        long sideEffectCount = sideEffectCounter.recordEffect();
        log.info("Simulated payment side effect applied: eventId={}, orderId={}, key={}, topic={}, partition={}, offset={}, consumerGroup={}, serviceInstance={}, sideEffectCount={}, idempotencyEnabled={}",
                record.value().eventId(),
                orderId,
                record.key(),
                record.topic(),
                record.partition(),
                record.offset(),
                groupId,
                serviceInstance,
                sideEffectCount,
                idempotencyEnabled);

        if (orderId.equals(failureAfterSideEffectOrderId)) {
            throw new IllegalStateException("Intentional failure after simulated side effect for orderId=" + orderId);
        }
    }

    private void delayProcessing() {
        if (processingDelayMs == 0) {
            return;
        }

        log.info("Delaying processing for {} ms before acknowledgement", processingDelayMs);
        try {
            Thread.sleep(processingDelayMs);
        }
        catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Processing interrupted before acknowledgement", exception);
        }
    }

}
