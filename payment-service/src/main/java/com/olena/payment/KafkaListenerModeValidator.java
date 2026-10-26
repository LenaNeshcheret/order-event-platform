package com.olena.payment;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Locale;
import java.util.Set;

@Component
class KafkaListenerModeValidator {
    private static final Set<String> AUTOMATIC_ACKNOWLEDGEMENT_MODES =
            Set.of("BATCH", "RECORD", "TIME", "COUNT", "COUNT_TIME");
    private static final Set<String> MANUAL_ACKNOWLEDGEMENT_MODES =
            Set.of("MANUAL", "MANUAL_IMMEDIATE");

    private final String listenerStyle;
    private final String acknowledgementMode;

    KafkaListenerModeValidator(
            @Value("${app.kafka.listener.style}") String listenerStyle,
            @Value("${spring.kafka.listener.ack-mode}") String acknowledgementMode
    ) {
        this.listenerStyle = listenerStyle;
        this.acknowledgementMode = acknowledgementMode;
    }

    @PostConstruct
    void validate() {
        String normalizedStyle = listenerStyle.toLowerCase(Locale.ROOT);
        String normalizedAcknowledgementMode = acknowledgementMode.toUpperCase(Locale.ROOT);

        if ("automatic".equals(normalizedStyle)
                && AUTOMATIC_ACKNOWLEDGEMENT_MODES.contains(normalizedAcknowledgementMode)) {
            return;
        }
        if ("manual".equals(normalizedStyle)
                && MANUAL_ACKNOWLEDGEMENT_MODES.contains(normalizedAcknowledgementMode)) {
            return;
        }

        throw new IllegalStateException("Unsupported payment listener style/acknowledgement-mode combination: "
                + listenerStyle + "/" + acknowledgementMode);
    }
}
