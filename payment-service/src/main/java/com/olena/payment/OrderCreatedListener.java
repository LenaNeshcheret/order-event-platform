package com.olena.payment;

import com.olena.events.OrderCreated;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
class OrderCreatedListener {
    private static final Logger log = LoggerFactory.getLogger(OrderCreatedListener.class);

    private final String groupId;
    private final String serviceInstance;

    OrderCreatedListener(
            @Value("${spring.kafka.consumer.group-id}") String groupId,
            @Value("${SERVICE_INSTANCE_ID:${HOSTNAME:payment-service}}") String serviceInstance
    ) {
        this.groupId = groupId;
        this.serviceInstance = serviceInstance;
    }

    @KafkaListener(topics = "order.created", groupId = "${spring.kafka.consumer.group-id}")
    void onOrderCreated(ConsumerRecord<String, OrderCreated> record) {
        log.info("Received OrderCreated: key={}, topic={}, partition={}, offset={}, consumerGroup={}, serviceInstance={}, orderId={}",
                record.key(),
                record.topic(),
                record.partition(),
                record.offset(),
                groupId,
                serviceInstance,
                record.value().payload().orderId());
    }
}
