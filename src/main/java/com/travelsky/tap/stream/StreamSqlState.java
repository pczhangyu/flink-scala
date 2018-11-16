package com.travelsky.tap.stream;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * 流sql处理例子
 * @author pczhangyu
 * @date 2018/11/12
 */
public class StreamSqlState {

    private static final int CHECK_INTERVAL = 30000;

    private static final String STATE_BACKEND_RECOVERY_PATH = "file:///opt/app/flink/recovery/check-points";

    public static void main(String[] args) throws Exception {

        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置检查点为30000毫秒
        env.enableCheckpointing(CHECK_INTERVAL);

        //设置backend方式为文件系统，路径为/opt/app/flink/recovery
        env.setStateBackend(new FsStateBackend(STATE_BACKEND_RECOVERY_PATH));

        StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
        //kafka config
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "10.221.130.226:9093");
        props.setProperty("group.id", String.format("%s-%s","zy",String.valueOf(System.currentTimeMillis())));
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>("test_all", new SimpleStringSchema(), props);

        SingleOutputStreamOperator<Order> orderSingleOutputStreamOperator = env.addSource(consumer).flatMap(new MessageSplitter());
        // register DataStream as Table
        tEnv.registerDataStream("OrderB", orderSingleOutputStreamOperator, "user, product, amount");
        // union the two tables
        Table result = tEnv.sqlQuery("SELECT * FROM OrderB WHERE amount < 2");

        tEnv.toAppendStream(result, Order.class).print();

        env.execute("state backend");
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
