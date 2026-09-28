package com.olena.order;

import com.olena.events.OrderItem;

import java.util.List;

public record CreateOrderRequest(String customerId, List<OrderItem> items, String currency) {
}
