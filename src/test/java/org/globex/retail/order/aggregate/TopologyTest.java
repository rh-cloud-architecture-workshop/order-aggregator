package org.globex.retail.order.aggregate;

import io.quarkus.kafka.client.serialization.ObjectMapperDeserializer;
import io.quarkus.test.junit.QuarkusTest;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.globex.retail.order.aggregate.model.AggregatedOrder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@QuarkusTest
public class TopologyTest {

    @Inject
    Topology topology;

    TopologyTestDriver testDriver;

    TestInputTopic<String, String> orderChangeEventTopic;

    TestInputTopic<String, String> lineItemChangeEventTopic;

    TestOutputTopic<Long, AggregatedOrder> aggregatedOrderTopic;

    @BeforeEach
    void setup() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "testApplicationId");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        testDriver = new TopologyTestDriver(topology, config);

        orderChangeEventTopic = testDriver.createInputTopic(
                "updates.order",
                new StringSerializer(),
                new StringSerializer()
        );

        lineItemChangeEventTopic = testDriver.createInputTopic(
                "updates.line-item",
                new StringSerializer(),
                new StringSerializer()
        );

        aggregatedOrderTopic = testDriver.createOutputTopic(
                "order.aggregated",
                new LongDeserializer(),
                new ObjectMapperDeserializer<>(AggregatedOrder.class)
        );
    }

    @AfterEach
    void tearDown() {
        //testDriver.getTimestampedKeyValueStore(SHOTS_ANALYSIS_STORE).flush();
        testDriver.close();
    }

    @Test
    void testOrderAggregation() {
        String orderChangeEventKey = "{\"id\":73}";
        String orderChangeEventValue = "{" +
                "   \"before\": null," +
                "   \"after\": {" +
                "      \"id\": 73," +
                "      \"customer_id\": \"zcole\"," +
                "      \"order_ts\": 1678479258884000" +
                "   }," +
                "   \"source\": {" +
                "      \"version\": \"2.1.1.Final\"," +
                "      \"connector\": \"postgresql\"," +
                "      \"name\": \"order.updates\"," +
                "      \"ts_ms\": 1678479258892," +
                "      \"snapshot\": \"false\"," +
                "      \"db\": \"orders\"," +
                "      \"sequence\": \"[\\\"23795224\\\",\\\"23795224\\\"]\"," +
                "      \"schema\": \"public\"," +
                "      \"table\": \"orders\"," +
                "      \"txId\": 593," +
                "      \"lsn\": 23795224," +
                "      \"xmin\": null" +
                "   }," +
                "   \"op\": \"c\"," +
                "   \"ts_ms\": 1678479258918," +
                "   \"transaction\": null" +
                "}";

        orderChangeEventTopic.pipeInput(orderChangeEventKey, orderChangeEventValue);

        String lineItemKey1 = " {\"id\":211}";
        String lineItemValue1 = "{" +
                "   \"before\": null," +
                "   \"after\": {" +
                "      \"id\": 211," +
                "      \"price\": \"13.50\"," +
                "      \"product_code\": \"RHNL-018\"," +
                "      \"quantity\": 1," +
                "      \"order_id\": 73" +
                "   }," +
                "   \"source\": {" +
                "      \"version\": \"2.1.1.Final\"," +
                "      \"connector\": \"postgresql\"," +
                "      \"name\": \"order.updates\"," +
                "      \"ts_ms\": 1678479258892," +
                "      \"snapshot\": \"false\"," +
                "      \"db\": \"orders\"," +
                "      \"sequence\": \"[\\\"23795224\\\",\\\"23795584\\\"]\"," +
                "      \"schema\": \"public\"," +
                "      \"table\": \"line_item\"," +
                "      \"txId\": 593," +
                "      \"lsn\": 23795584," +
                "      \"xmin\": null" +
                "   }," +
                "   \"op\": \"c\"," +
                "   \"ts_ms\": 1678479258919," +
                "   \"transaction\": null" +
                "}";

        String lineItemKey2 = "{\"id\":212}";
        String lineItemValue2 = "{" +
                "   \"before\": null," +
                "   \"after\": {" +
                "      \"id\": 212," +
                "      \"price\": \"2.75\"," +
                "      \"product_code\": \"RHNAM-249\"," +
                "      \"quantity\": 1," +
                "      \"order_id\": 73" +
                "   }," +
                "   \"source\": {" +
                "      \"version\": \"2.1.1.Final\"," +
                "      \"connector\": \"postgresql\"," +
                "      \"name\": \"order.updates\"," +
                "      \"ts_ms\": 1678479258892," +
                "      \"snapshot\": \"false\"," +
                "      \"db\": \"orders\"," +
                "      \"sequence\": \"[\\\"23795224\\\",\\\"23795744\\\"]\"," +
                "      \"schema\": \"public\"," +
                "      \"table\": \"line_item\"," +
                "      \"txId\": 593," +
                "      \"lsn\": 23795744," +
                "      \"xmin\": null" +
                "   }," +
                "   \"op\": \"c\"," +
                "   \"ts_ms\": 1678479258919," +
                "   \"transaction\": null" +
                "}";

        lineItemChangeEventTopic.pipeInput(lineItemKey1, lineItemValue1);
        lineItemChangeEventTopic.pipeInput(lineItemKey2, lineItemValue2);

        TestRecord<Long, AggregatedOrder> record = aggregatedOrderTopic.readRecord();
        assertThat(record, notNullValue());
        assertThat(record.getKey(), is(73L));
        assertThat(record.getValue().getOrderId(), is(73L));
        assertThat(record.getValue().getCustomer(), is("zcole"));
        assertThat(record.getValue().getTimestamp(), is(1678479258884000L));
        assertThat(record.getValue().getOrderTotal(), is(13.50));

        TestRecord<Long, AggregatedOrder> record2 = aggregatedOrderTopic.readRecord();
        assertThat(record2, notNullValue());
        assertThat(record2.getKey(), is(73L));
        assertThat(record2.getValue().getOrderId(), is(73L));
        assertThat(record2.getValue().getCustomer(), is("zcole"));
        assertThat(record2.getValue().getTimestamp(), is(1678479258884000L));
        assertThat(record2.getValue().getOrderTotal(), is(16.25));
    }
}
