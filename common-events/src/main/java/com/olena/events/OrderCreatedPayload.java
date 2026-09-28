package com.olena.events;

import java.math.BigDecimal;
import java.util.List;

public record OrderCreatedPayload(
        String orderId,
        String customerId,
        List<OrderItem> items,
        BigDecimal totalAmount,
        String currency
) {
}
