package com.travelsky.tap.stream;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.*;

/**
 *
 * @author pczhangyu
 * @date 2018/10/24
 */
public class FilnkCostKafkaWindow {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 设置时间的角色
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.221.130.226:9093");
        properties.setProperty("zookeeper.connect", "10.221.130.226:2181,10.221.130.228:2181,10.221.130.230:2181/kafka-zkroot-test");
        properties.setProperty("group.id", String.format("%s-%s","zy",String.valueOf(System.currentTimeMillis())));

        FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<String>("test_all", new SimpleStringSchema(),
                properties);
//        myConsumer.assignTimestampsAndWatermarks();//时间戳水印线
        DataStream<String> stream = env.addSource(myConsumer);
        DataStream<Tuple2<String, Integer>> counts = stream.
                flatMap(new LineSplitter())
                .keyBy(0)
                .window(EventTimeSessionWindows.withGap(Time.minutes(10)))

//                .trigger(Trigger<Tuple2<String,Integer>>, WindowAssigner)
                .sum(1);
//                .timeWindow(Time.seconds(30)).sum(1);

//                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
//                    @Override
//                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t0, Tuple2<String, Integer> t1) throws Exception {
//                        Integer field = t0.getField(1);
//                        Integer field1 = t1.getField(1);
//                        return new Tuple2<String, Integer>(((String) t0.getField(0)),field+field1);
//                    }
//                });
        List<HttpHost> httpHost = new ArrayList<>();
        httpHost.add(new HttpHost("10.221.130.226", 9200, "http"));
        // use a ElasticsearchSink.Builder to create an ElasticsearchSink
        ElasticsearchSink.Builder<Tuple2<String, Integer>> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHost,
                new ElasticsearchSinkFunction<Tuple2<String, Integer>>() {
                    @Override
                    public void process(Tuple2<String, Integer> stringIntegerTuple2, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                        JSONObject doc = new JSONObject();
                        doc.put("word",stringIntegerTuple2.getField(0));
                        doc.put("countSum",stringIntegerTuple2.getField(1));
                        requestIndexer.add(createIndexRequest(doc.toJSONString()));
                    }

                    private IndexRequest createIndexRequest(String element) {
                        Map<String, String> json = new HashMap<>();
                        json.put("data", element);
                        return Requests.indexRequest()
                                .index("flink-result-session")
                                .type("wordcount")
                                .source(json);
                    }

//                    @Override
//                    public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
//                        indexer.add(createIndexRequest(element));
//                    }
                }
        );

        // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
        esSinkBuilder.setBulkFlushMaxActions(1);

        counts.addSink(esSinkBuilder.build());
        // provide a RestClientFactory for custom configuration on the internally created REST client
        counts.print();
        env.execute("session window consumer Kafka data sink to elasticsearch");
    }

    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            String[] tokens = value.toLowerCase().split("\\W+");
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }

}
