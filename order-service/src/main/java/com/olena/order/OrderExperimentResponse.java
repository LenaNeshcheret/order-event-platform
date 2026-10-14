package com.olena.order;

import java.util.List;

public record OrderExperimentResponse(
        String orderId,
        boolean withoutKey,
        List<PublishedRecord> records
) {
}
