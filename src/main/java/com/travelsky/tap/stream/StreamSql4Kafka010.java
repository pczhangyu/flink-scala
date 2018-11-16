package com.travelsky.tap.stream;

import com.alibaba.fastjson.JSONObject;
import com.travelsky.tap.sink.ElasticsearchSinker;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 *  kafka 0.10 sql query
 * @author pczhangyu
 * @date 2018/11/12
 */
public class StreamSql4Kafka010 {


    public static void main(String[] args) throws Exception {

        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "10.221.130.226:9093");
        props.setProperty("group.id", String.format("%s-%s","zy",String.valueOf(System.currentTimeMillis())));
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        FlinkKafkaConsumer010<String> consumer =
                new FlinkKafkaConsumer010<>("test_all", new SimpleStringSchema(), props);

        SingleOutputStreamOperator<Order> orderSingleOutputStreamOperator = env.addSource(consumer).flatMap(new MessageSplitter());

//        SingleOutputStreamOperator<Order> orderSingleOutputStreamOperator2 = env.addSource(consumer).flatMap(new MessageSplitter());
        // convert DataStream to Table
        Table tableA = tEnv.fromDataStream(orderSingleOutputStreamOperator, "user, product, amount");
        // register DataStream as Table
//        tEnv.registerDataStream("OrderB", orderSingleOutputStreamOperator2, "user, product, amount");
        Table result = tEnv.sqlQuery("SELECT * FROM " + tableA + " WHERE amount > 300");
//        2 UNION ALL " +
//        "SELECT * FROM OrderB WHERE amount < 2
        DataStream<Order> orderDataStream = tEnv.toAppendStream(result, Order.class);
        List<HttpHost> httpHost = new ArrayList<>();
        httpHost.add(new HttpHost("10.221.130.226", 9200, "http"));
        // use a ElasticsearchSink.Builder to create an ElasticsearchSink
        ElasticsearchSink.Builder<StreamSql4Kafka010.Order> esSinkBuilder = new ElasticsearchSink.Builder<StreamSql4Kafka010.Order>(
                httpHost,
                new ElasticsearchSinkFunction<StreamSql4Kafka010.Order>() {
                    @Override
                    public void process(StreamSql4Kafka010.Order stringIntegerTuple2, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                        Object order = JSONObject.toJSON(stringIntegerTuple2);
                        requestIndexer.add(createIndexRequest(((JSONObject) order)));
                    }

                    public IndexRequest createIndexRequest(JSONObject jsonObject) {
                        return Requests.indexRequest()
                                .index("flink-sql-result")
                                .type("stream")
                                .source(jsonObject);
                    }

                }
        );
        esSinkBuilder.setBulkFlushMaxActions(1);
        orderDataStream.addSink(esSinkBuilder.build());
//                .keyBy("f1")
//                .timeWindow(Time.seconds(30)).sum(0);
//                .addSink()
        env.execute("stream sql test/single table");
    }

    /**
     * Simple POJO.
     */
    public static class Order {
        public Long user;
        public String product;
        public int amount;

        public Order() {
        }

        public Order(Long user, String product, int amount) {
            this.user = user;
            this.product = product;
            this.amount = amount;
        }

        @Override
        public String toString() {
            return "Order{" +
                    "user=" + user +
                    ", product='" + product + '\'' +
                    ", amount=" + amount +
                    '}';
        }
    }
    private static class MessageSplitter implements FlatMapFunction<String,Order> {

        @Override
        public void flatMap(String s, Collector<Order> collector) throws Exception {
            if (s != null ) {
                Order order = JSONObject.parseObject(s, Order.class);
                collector.collect(order);
            }
        }
    }
}
