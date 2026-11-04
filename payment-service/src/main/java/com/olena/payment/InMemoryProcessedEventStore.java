package com.olena.payment;

import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
class InMemoryProcessedEventStore {
    private final ConcurrentMap<UUID, Boolean> processedEventIds = new ConcurrentHashMap<>();

    boolean markProcessedIfFirstDelivery(UUID eventId) {
        return processedEventIds.putIfAbsent(eventId, Boolean.TRUE) == null;
    }
}
