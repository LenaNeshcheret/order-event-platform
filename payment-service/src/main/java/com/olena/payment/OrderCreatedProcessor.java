package com.olena.payment;

import com.olena.events.OrderCreated;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
class OrderCreatedProcessor {
    private static final Logger log = LoggerFactory.getLogger(OrderCreatedProcessor.class);

    private final String groupId;
    private final String serviceInstance;
    private final String acknowledgementMode;
    private final long processingDelayMs;
    private final String failureOrderId;
    private final long failureOffset;

    OrderCreatedProcessor(
            @Value("${spring.kafka.consumer.group-id}") String groupId,
            @Value("${SERVICE_INSTANCE_ID:${HOSTNAME:payment-service}}") String serviceInstance,
            @Value("${spring.kafka.listener.ack-mode}") String acknowledgementMode,
            @Value("${app.kafka.listener.processing-delay-ms}") long processingDelayMs,
            @Value("${app.kafka.listener.failure-order-id}") String failureOrderId,
            @Value("${app.kafka.listener.failure-offset}") long failureOffset
    ) {
        this.groupId = groupId;
        this.serviceInstance = serviceInstance;
        this.acknowledgementMode = acknowledgementMode;
        this.processingDelayMs = processingDelayMs;
        this.failureOrderId = failureOrderId;
        this.failureOffset = failureOffset;
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
        if (record.value().payload().orderId().equals(failureOrderId)) {
            throw new IllegalStateException("Intentional poison record for orderId=" + failureOrderId);
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
