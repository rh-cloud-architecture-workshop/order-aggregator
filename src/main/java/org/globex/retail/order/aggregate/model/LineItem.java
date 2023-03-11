package org.globex.retail.order.aggregate.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LineItem {

    @JsonProperty("id")
    private long lineItemId;

    @JsonProperty("order_id")
    private long orderId;

    @JsonProperty("product_code")
    private String product;

    @JsonProperty("quantity")
    private int quantity;

    @JsonProperty("price")
    private double price;

    public long getLineItemId() {
        return lineItemId;
    }

    public long getOrderId() {
        return orderId;
    }

    public String getProduct() {
        return product;
    }

    public int getQuantity() {
        return quantity;
    }

    public double getPrice() {
        return price;
    }
}
