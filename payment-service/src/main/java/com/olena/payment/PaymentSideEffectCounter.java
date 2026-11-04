package com.olena.payment;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

@Component
class PaymentSideEffectCounter {
    private final AtomicLong count = new AtomicLong();

    long recordEffect() {
        return count.incrementAndGet();
    }

    long currentCount() {
        return count.get();
    }
}
