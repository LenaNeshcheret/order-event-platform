package com.olena.events;

import java.time.Instant;
import java.util.UUID;

public record OrderCreated(
        UUID eventId,
        String eventType,
        int eventVersion,
        Instant occurredAt,
        UUID correlationId,
        OrderCreatedPayload payload
) {
    public static final String EVENT_TYPE = "OrderCreated";
    public static final int EVENT_VERSION = 1;
}
