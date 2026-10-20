package com.olena.payment;

import com.olena.events.OrderCreated;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
class OrderCreatedListener {
    private static final Logger log = LoggerFactory.getLogger(OrderCreatedListener.class);

    private final String groupId;
    private final String serviceInstance;
    private final String acknowledgementMode;
    private final long processingDelayMs;
    private final String failureOrderId;

    OrderCreatedListener(
            @Value("${spring.kafka.consumer.group-id}") String groupId,
            @Value("${SERVICE_INSTANCE_ID:${HOSTNAME:payment-service}}") String serviceInstance,
            @Value("${spring.kafka.listener.ack-mode}") String acknowledgementMode,
            @Value("${app.kafka.listener.processing-delay-ms}") long processingDelayMs,
            @Value("${app.kafka.listener.failure-order-id}") String failureOrderId
    ) {
        this.groupId = groupId;
        this.serviceInstance = serviceInstance;
        this.acknowledgementMode = acknowledgementMode;
        this.processingDelayMs = processingDelayMs;
        this.failureOrderId = failureOrderId;
    }

    @KafkaListener(
            topics = "order.created",
            groupId = "${spring.kafka.consumer.group-id}",
            concurrency = "${app.kafka.listener.concurrency}"
    )
    void onOrderCreated(ConsumerRecord<String, OrderCreated> record, Acknowledgment acknowledgment) {
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

        if (record.value().payload().orderId().equals(failureOrderId)) {
            log.warn("Failing before acknowledgement for orderId={}", failureOrderId);
            throw new IllegalStateException("Lab 04 failure before acknowledgement for orderId=" + failureOrderId);
        }

        if (usesManualAcknowledgement()) {
            if (acknowledgment == null) {
                throw new IllegalStateException("Manual acknowledgement mode requires an Acknowledgment");
            }
            acknowledgment.acknowledge();
            log.info("Manually acknowledged OrderCreated: topic={}, partition={}, offset={}, consumerGroup={}, serviceInstance={}",
                    record.topic(), record.partition(), record.offset(), groupId, serviceInstance);
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

    private boolean usesManualAcknowledgement() {
        return "MANUAL".equalsIgnoreCase(acknowledgementMode)
                || "MANUAL_IMMEDIATE".equalsIgnoreCase(acknowledgementMode);
    }
}
