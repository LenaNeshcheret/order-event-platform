package com.olena.payment;

import com.olena.events.OrderCreated;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(
        name = "app.kafka.listener.style",
        havingValue = "automatic",
        matchIfMissing = true
)
class AutomaticOrderCreatedListener {
    private final OrderCreatedProcessor processor;

    AutomaticOrderCreatedListener(OrderCreatedProcessor processor) {
        this.processor = processor;
    }

    @KafkaListener(
            topics = "order.created",
            groupId = "${spring.kafka.consumer.group-id}",
            concurrency = "${app.kafka.listener.concurrency}"
    )
    void onOrderCreated(ConsumerRecord<String, OrderCreated> record) {
        processor.process(record);
    }
}
