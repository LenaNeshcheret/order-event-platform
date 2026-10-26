package com.olena.payment;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
class KafkaFailureConfiguration {
    private static final Logger log = LoggerFactory.getLogger(KafkaFailureConfiguration.class);

    @Bean
    DefaultErrorHandler kafkaErrorHandler(
            @Value("${app.kafka.listener.retry.backoff-ms}") long backoffMs,
            @Value("${app.kafka.listener.retry.max-retries}") long maxRetries
    ) {
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                (record, exception) -> log.error(
                        "Retries exhausted; skipping poison record: key={}, topic={}, partition={}, offset={}, exceptionType={}, exceptionMessage={}",
                        record.key(),
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        exception.getClass().getSimpleName(),
                        exception.getMessage()),
                new FixedBackOff(backoffMs, maxRetries)
        );

        errorHandler.setRetryListeners(new RetryListener() {
            @Override
            public void failedDelivery(ConsumerRecord<?, ?> record, Exception exception, int deliveryAttempt) {
                log.warn(
                        "Listener failure: key={}, topic={}, partition={}, offset={}, deliveryAttempt={}, exceptionType={}, exceptionMessage={}",
                        record.key(),
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        deliveryAttempt,
                        exception.getClass().getSimpleName(),
                        exception.getMessage());
            }
        });
        return errorHandler;
    }
}
