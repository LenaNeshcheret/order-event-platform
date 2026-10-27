package com.olena.payment;

import com.olena.events.OrderCreated;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.RetryTopicHeaders;
import org.springframework.kafka.retrytopic.SameIntervalTopicReuseStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

@Component
@ConditionalOnProperty(
        name = "app.kafka.listener.style",
        havingValue = "automatic",
        matchIfMissing = true
)
class AutomaticOrderCreatedListener {
    private static final Logger log = LoggerFactory.getLogger(AutomaticOrderCreatedListener.class);

    private final OrderCreatedProcessor processor;
    private final String groupId;
    private final String serviceInstance;

    AutomaticOrderCreatedListener(
            OrderCreatedProcessor processor,
            @Value("${spring.kafka.consumer.group-id}") String groupId,
            @Value("${SERVICE_INSTANCE_ID:${HOSTNAME:payment-service}}") String serviceInstance
    ) {
        this.processor = processor;
        this.groupId = groupId;
        this.serviceInstance = serviceInstance;
    }

    @RetryableTopic(
            attempts = "${app.kafka.listener.retry.attempts}",
            backOff = @BackOff(delayString = "${app.kafka.listener.retry.backoff-ms}"),
            autoCreateTopics = "true",
            numPartitions = "3",
            replicationFactor = "1",
            retryTopicSuffix = "-retry",
            dltTopicSuffix = "-dlt",
            sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.SINGLE_TOPIC,
            dltStrategy = DltStrategy.FAIL_ON_ERROR
    )
    @KafkaListener(
            topics = "order.created",
            groupId = "${spring.kafka.consumer.group-id}",
            concurrency = "${app.kafka.listener.concurrency}"
    )
    void onOrderCreated(ConsumerRecord<String, OrderCreated> record) {
        processor.process(record);
    }

    @DltHandler
    void onDeadLetter(ConsumerRecord<String, OrderCreated> record) {
        log.error("DLT record received: key={}, dltTopic={}, dltPartition={}, dltOffset={}, orderId={}, originalTopic={}, originalPartition={}, originalOffset={}, deliveryAttempt={}, exceptionMessage={}, consumerGroup={}, serviceInstance={}",
                record.key(),
                record.topic(),
                record.partition(),
                record.offset(),
                record.value().payload().orderId(),
                stringHeader(record, KafkaHeaders.ORIGINAL_TOPIC),
                integerHeader(record, KafkaHeaders.ORIGINAL_PARTITION),
                longHeader(record, KafkaHeaders.ORIGINAL_OFFSET),
                integerHeader(record, RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS),
                stringHeader(record, KafkaHeaders.EXCEPTION_MESSAGE),
                groupId,
                serviceInstance);
    }

    private static String stringHeader(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? "unknown" : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static int integerHeader(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null) {
            return -1;
        }
        byte[] value = header.value();
        return value.length == Byte.BYTES ? value[0] : ByteBuffer.wrap(value).getInt();
    }

    private static long longHeader(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? -1 : ByteBuffer.wrap(header.value()).getLong();
    }
}
