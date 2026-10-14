package com.olena.order;

public record OrderExperimentRequest(String orderId, int count, boolean withoutKey) {
}
