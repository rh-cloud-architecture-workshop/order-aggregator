package org.globex.retail.order.aggregate.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Order {

    @JsonProperty("id")
    private long orderId;

    @JsonProperty("customer_id")
    private String customer;

    @JsonProperty("order_ts")
    private long timestamp;

    public long getOrderId() {
        return orderId;
    }

    public String getCustomer() {
        return customer;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
