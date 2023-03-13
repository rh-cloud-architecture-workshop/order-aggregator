package org.globex.retail.order.aggregate.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.Instant;

public class AggregatedOrder {

    @JsonProperty("orderId")
    private long orderId;

    @JsonProperty("customer")
    private String customer;

    @JsonProperty("date")
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ", timezone = "UTC")
    private Instant date;

    @JsonProperty("total")
    private Double orderTotal;

    public long getOrderId() {
        return orderId;
    }

    public String getCustomer() {
        return customer;
    }

    public Instant getDate() {
        return date;
    }

    public double getOrderTotal() {
        return orderTotal;
    }

    public AggregatedOrder addLineItem(OrderAndLineItem orderAndLineItem) {
        this.orderId = orderAndLineItem.getOrder().getOrderId();
        this.customer = orderAndLineItem.getOrder().getCustomer();
        this.date = Instant.ofEpochMilli(orderAndLineItem.getOrder().getTimestamp()/1000);
        if (this.orderTotal == null) {
            this.orderTotal = 0.0;
        }
        this.orderTotal = BigDecimal.valueOf(orderTotal)
                .add(BigDecimal.valueOf(orderAndLineItem.getLineItem().getPrice())
                        .multiply(new BigDecimal(orderAndLineItem.getLineItem().getQuantity()))).doubleValue();
        return this;
    }

    public AggregatedOrder removeLineItem(OrderAndLineItem orderAndLineItem) {
        if (this.orderTotal == null) {
            this.orderTotal = 0.0;
        }
        this.orderTotal = BigDecimal.valueOf(orderTotal)
                .subtract(BigDecimal.valueOf(orderAndLineItem.getLineItem().getPrice())
                        .multiply(new BigDecimal(orderAndLineItem.getLineItem().getQuantity()))).doubleValue();
        return this;
    }
}
