package org.globex.retail.order.aggregate.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class OrderAndLineItem {

    @JsonProperty("order")
    private Order order;

    @JsonProperty("lineItem")
    private LineItem lineItem;

    public OrderAndLineItem(Order order, LineItem lineItem) {
        this.order = order;
        this.lineItem = lineItem;
    }

    public Order getOrder() {
        return order;
    }

    public LineItem getLineItem() {
        return lineItem;
    }
}
