package com.olena.payment;

import com.olena.events.OrderCreated;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "app.kafka.listener.style", havingValue = "manual")
class ManualOrderCreatedListener {
    private static final Logger log = LoggerFactory.getLogger(ManualOrderCreatedListener.class);

    private final OrderCreatedProcessor processor;
    private final String groupId;
    private final String serviceInstance;
    private final String acknowledgementMode;

    ManualOrderCreatedListener(
            OrderCreatedProcessor processor,
            @Value("${spring.kafka.consumer.group-id}") String groupId,
            @Value("${SERVICE_INSTANCE_ID:${HOSTNAME:payment-service}}") String serviceInstance,
            @Value("${spring.kafka.listener.ack-mode}") String acknowledgementMode
    ) {
        this.processor = processor;
        this.groupId = groupId;
        this.serviceInstance = serviceInstance;
        this.acknowledgementMode = acknowledgementMode;
    }

    @KafkaListener(
            topics = "order.created",
            groupId = "${spring.kafka.consumer.group-id}",
            concurrency = "${app.kafka.listener.concurrency}"
    )
    void onOrderCreated(ConsumerRecord<String, OrderCreated> record, Acknowledgment acknowledgment) {
        processor.process(record);
        acknowledgment.acknowledge();
        log.info("Acknowledgment submitted: topic={}, partition={}, offset={}, consumerGroup={}, serviceInstance={}, acknowledgementMode={}",
                record.topic(), record.partition(), record.offset(), groupId, serviceInstance, acknowledgementMode);
    }
}
