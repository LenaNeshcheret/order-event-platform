package com.olena.payment;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryProcessedEventStoreTest {

    @Test
    void acceptsAnEventIdOnlyOnce() {
        InMemoryProcessedEventStore store = new InMemoryProcessedEventStore();
        UUID eventId = UUID.randomUUID();

        assertThat(store.markProcessedIfFirstDelivery(eventId)).isTrue();
        assertThat(store.markProcessedIfFirstDelivery(eventId)).isFalse();
    }
}
