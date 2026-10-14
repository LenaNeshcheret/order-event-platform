package com.olena.order;

public record PublishedRecord(String key, String topic, int partition, long offset) {
}
