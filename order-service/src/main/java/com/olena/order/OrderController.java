package com.olena.order;

import com.olena.events.OrderCreated;
import com.olena.events.OrderCreatedPayload;
import com.olena.events.OrderItem;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@RestController
@RequestMapping("/orders")
class OrderController {
    private static final Logger log = LoggerFactory.getLogger(OrderController.class);
    private static final String ORDER_CREATED_TOPIC = "order.created";

    private final KafkaTemplate<String, OrderCreated> kafkaTemplate;

    OrderController(KafkaTemplate<String, OrderCreated> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    OrderAcceptedResponse createOrder(@RequestBody CreateOrderRequest request) {
        String orderId = UUID.randomUUID().toString();
        OrderCreated event = new OrderCreated(
                UUID.randomUUID(),
                OrderCreated.EVENT_TYPE,
                OrderCreated.EVENT_VERSION,
                Instant.now(),
                UUID.randomUUID(),
                new OrderCreatedPayload(
                        orderId,
                        request.customerId(),
                        request.items(),
                        totalAmount(request),
                        request.currency()
                )
        );

        kafkaTemplate.send(ORDER_CREATED_TOPIC, orderId, event)
                .whenComplete((result, exception) -> {
                    if (exception != null) {
                        log.error("Could not publish OrderCreated for orderId={}", orderId, exception);
                        return;
                    }
                    RecordMetadata metadata = result.getRecordMetadata();
                    log.info("Published OrderCreated: key={}, topic={}, partition={}, offset={}",
                            orderId, metadata.topic(), metadata.partition(), metadata.offset());
                });

        return new OrderAcceptedResponse(orderId, "accepted");
    }

    private BigDecimal totalAmount(CreateOrderRequest request) {
        return request.items().stream()
                .map(item -> item.unitPrice().multiply(BigDecimal.valueOf(item.quantity())))
                .reduce(BigDecimal.ZERO, BigDecimal::add);
    }
}
