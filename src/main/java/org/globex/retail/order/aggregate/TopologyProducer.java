package org.globex.retail.order.aggregate;

import io.debezium.serde.DebeziumSerdes;
import io.debezium.serde.json.JsonSerdeConfig;
import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.globex.retail.order.aggregate.model.AggregatedOrder;
import org.globex.retail.order.aggregate.model.LineItem;
import org.globex.retail.order.aggregate.model.Order;
import org.globex.retail.order.aggregate.model.OrderAndLineItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import java.util.Collections;

@ApplicationScoped
public class TopologyProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopologyProducer.class);

    @ConfigProperty(name = "topic.order-change-event")
    String orderChangeEventTopic;

    @ConfigProperty(name = "topic.lineitem-change-event")
    String lineItemChangeEventTopic;

    @ConfigProperty(name = "topic.aggregate-order")
    String aggregatedOrderTopic;

    @Produces
    public Topology buildTopology() {

        StreamsBuilder builder = new StreamsBuilder();

        final Serde<Long> orderKeySerde = DebeziumSerdes.payloadJson(Long.class);
        orderKeySerde.configure(Collections.emptyMap(), true);
        final Serde<Order> orderSerde = DebeziumSerdes.payloadJson(Order.class);
        orderSerde.configure(Collections.singletonMap(JsonSerdeConfig.FROM_FIELD.name(), "after"), false);

        final Serde<Long> lineItemKeySerde = DebeziumSerdes.payloadJson(Long.class);
        lineItemKeySerde.configure(Collections.emptyMap(), true);
        final Serde<LineItem> lineItemSerde = DebeziumSerdes.payloadJson(LineItem.class);
        lineItemSerde.configure(Collections.singletonMap(JsonSerdeConfig.FROM_FIELD.name(), "after"), false);

        final Serde<OrderAndLineItem> orderAndLineItemSerde = new ObjectMapperSerde<>(OrderAndLineItem.class);

        final Serde<AggregatedOrder> aggregatedOrderSerde = new ObjectMapperSerde<>(AggregatedOrder.class);


        // KTable of Order events
        KTable<Long, Order> orderTable = builder.table(orderChangeEventTopic, Consumed.with(orderKeySerde, orderSerde));

        // KTable of Lineitem events
        KTable<Long, LineItem> lineItemTable = builder.table(lineItemChangeEventTopic, Consumed.with(lineItemKeySerde, lineItemSerde));

        // Join LineItem events with Order events by foreign key, aggregate Linetem price in Order
        KTable<Long, AggregatedOrder> aggregatedOrders = lineItemTable
                .join(orderTable, LineItem::getOrderId, (lineItem, order) -> new OrderAndLineItem(order, lineItem),
                        Materialized.with(Serdes.Long(), orderAndLineItemSerde))
                .groupBy((key, value) -> KeyValue.pair(value.getOrder().getOrderId(), value),
                        Grouped.with(Serdes.Long(), orderAndLineItemSerde))
                .aggregate(AggregatedOrder::new, (key, value, aggregate) -> aggregate.addLineItem(value),
                        (key, value, aggregate) -> aggregate.removeLineItem(value),
                        Materialized.with(Serdes.Long(), aggregatedOrderSerde));

        aggregatedOrders.toStream().to(aggregatedOrderTopic, Produced.with(Serdes.Long(), aggregatedOrderSerde));

        Topology topology = builder.build();
        LOGGER.debug(topology.describe().toString());
        return topology;
    }

}
