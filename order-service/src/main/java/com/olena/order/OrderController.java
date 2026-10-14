package com.olena.order;

import com.olena.events.OrderCreated;
import com.olena.events.OrderCreatedPayload;
import com.olena.events.OrderItem;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

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

    @PostMapping("/experiments")
    @ResponseStatus(HttpStatus.CREATED)
    OrderExperimentResponse publishExperiment(@RequestBody OrderExperimentRequest request)
            throws ExecutionException, InterruptedException {
        validateExperiment(request);

        String recordKey = request.withoutKey() ? null : request.orderId();
        List<PublishedRecord> records = new ArrayList<>(request.count());

        for (int index = 0; index < request.count(); index++) {
            OrderCreated event = orderCreated(
                    request.orderId(),
                    "lab02-customer",
                    List.of(new OrderItem("lab02-product", 1, new BigDecimal("1.00"))),
                    new BigDecimal("1.00"),
                    "EUR"
            );
            SendResult<String, OrderCreated> result = kafkaTemplate.send(ORDER_CREATED_TOPIC, recordKey, event).get();
            RecordMetadata metadata = result.getRecordMetadata();
            logPublished(recordKey, metadata);
            records.add(new PublishedRecord(recordKey, metadata.topic(), metadata.partition(), metadata.offset()));
        }

        return new OrderExperimentResponse(request.orderId(), request.withoutKey(), records);
    }

    private OrderCreated orderCreated(
            String orderId,
            String customerId,
            List<OrderItem> items,
            BigDecimal totalAmount,
            String currency
    ) {
        return new OrderCreated(
                UUID.randomUUID(),
                OrderCreated.EVENT_TYPE,
                OrderCreated.EVENT_VERSION,
                Instant.now(),
                UUID.randomUUID(),
                new OrderCreatedPayload(orderId, customerId, items, totalAmount, currency)
        );
    }

    private void validateExperiment(OrderExperimentRequest request) {
        if (request.orderId() == null || request.orderId().isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "orderId must not be blank");
        }
        if (request.count() < 1 || request.count() > 1_000) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "count must be between 1 and 1000");
        }
    }

    private void logPublished(String recordKey, RecordMetadata metadata) {
        log.info("Published OrderCreated: key={}, topic={}, partition={}, offset={}",
                recordKey, metadata.topic(), metadata.partition(), metadata.offset());
    }

    private BigDecimal totalAmount(CreateOrderRequest request) {
        return request.items().stream()
                .map(item -> item.unitPrice().multiply(BigDecimal.valueOf(item.quantity())))
                .reduce(BigDecimal.ZERO, BigDecimal::add);
    }
}
