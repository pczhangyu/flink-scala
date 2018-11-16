package com.travelsky.tap.stream;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 *
 * @author pczhangyu
 * @date 2018/10/28
 */
public class Kafka010NumCountConsumerMultKey {

    private static final Logger log = LoggerFactory.getLogger(Kafka010NumCountConsumerMultKey.class);

    private static final String FORMAT_TIME = "yyyy/dd/MM HH:mm:ss";

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(35000);
        // 设置启动检查点！！
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        log.info("\n\n启动\n\n");
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "10.221.130.226:9093");
        props.setProperty("group.id", String.format("%s-%s","zy",String.valueOf(System.currentTimeMillis())));
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        FlinkKafkaConsumer010<String> consumer =
                new FlinkKafkaConsumer010<>("test_all", new SimpleStringSchema(), props);
        consumer.assignTimestampsAndWatermarks(new MessageWaterEmitter());

        DataStream<Tuple7<String,String,String, Integer, String,Integer,Integer>> keyedStream = env
                .addSource(consumer)
                .flatMap(new MessageSplitter())
                .keyBy(0,2)
                .timeWindow(Time.seconds(30))
                .reduce(new ReduceFunction<Tuple7<String,String,String, Integer, String,Integer,Integer>>() {
                    @Override
                    public Tuple7<String,String,String, Integer, String,Integer,Integer> reduce(Tuple7<String,String,String, Integer, String,Integer,Integer> t0, Tuple7<String,String,String, Integer, String,Integer,Integer> t1) throws Exception {
                        String time0 = t0.getField(4);
                        String time1 = t1.getField(4);
                        Integer count0 = t0.getField(3);
                        Integer count1 = t1.getField(3);
                        Integer ms0 = t0.getField(6);
                        Integer ms1 = t1.getField(6);
                        if (time0.contains("000")){
                            time0 = new java.text.SimpleDateFormat(FORMAT_TIME).format(Long.valueOf(time0));
                        }
                        if (!time0.contains("ms")){
                            time0 = time0.concat(" took"+ms0+"ms");
                        }
                        String date1 = new java.text.SimpleDateFormat(FORMAT_TIME).format(Long.valueOf(time1));
                        return new Tuple7<>(((String) t0.getField(0)),((String) t0.getField(1)),((String) t0.getField(2)), count0 + count1, time0 +"|"+ date1.concat(" took"+ms1+"ms"), ((Integer) t0.getField(5)),(ms0+ms1)/2);
                    }
                });

        List<HttpHost> httpHost = new ArrayList<>();
        httpHost.add(new HttpHost("10.221.130.226", 9200, "http"));
        // use a ElasticsearchSink.Builder to create an ElasticsearchSink
        ElasticsearchSink.Builder<Tuple7<String,String,String, Integer, String,Integer,Integer>> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHost,
                new ElasticsearchSinkFunction<Tuple7<String,String,String, Integer, String,Integer,Integer>>() {
                    @Override
                    public void process(Tuple7<String,String,String, Integer, String,Integer,Integer> stringIntegerTuple2, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                        JSONObject doc = new JSONObject();
                        doc.put("url",stringIntegerTuple2.getField(0));
                        doc.put("method",stringIntegerTuple2.getField(1));
                        doc.put("response",stringIntegerTuple2.getField(2));
                        doc.put("count",stringIntegerTuple2.getField(3));
                        doc.put("code",stringIntegerTuple2.getField(5));
                        doc.put("responseTimeAvg",stringIntegerTuple2.getField(6));
                        String timeListStr = stringIntegerTuple2.getField(4);
                        List<String> timeList = Arrays.asList(timeListStr.split("\\|"));
                        doc.put("times",timeList);
                        requestIndexer.add(createIndexRequest(doc));
                    }

                    public IndexRequest createIndexRequest(JSONObject jsonObject) {
                        return Requests.indexRequest()
                                .index("flink-result-success")
                                .type("wordcount")
                                .source(jsonObject);
                    }

                }
        );

        // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
        esSinkBuilder.setBulkFlushMaxActions(1);
        keyedStream.addSink(esSinkBuilder.build());
        keyedStream.print();
        env.execute("success response count");
    }

    private static class MessageWaterEmitter implements AssignerWithPunctuatedWatermarks<String> {

        /*
         * 再执行该函数，extractedTimestamp的值是extractTimestamp的返回值
         */
        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(String lastElement, long extractedTimestamp) {
            if (lastElement != null && lastElement.contains(",")) {
                String[] parts = lastElement.split(",");
                if(parts.length==7) {
                    try {
                        log.info("lastElement={},and time={}", lastElement, parts[4]);
                        return new Watermark(Long.valueOf(parts[4]));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

            }
            return null;
        }

        /*
         * 先执行该函数，从element中提取时间戳
         * previousElementTimestamp 是当前的时间
         */
        @Override
        public long extractTimestamp(String element, long previousElementTimestamp) {
            if (element != null && element.contains(",")) {
                String[] parts = element.split(",");
                if (parts.length == 7) {
                    try {
                        log.info("extract={},privious={},curr={}", element, previousElementTimestamp, parts[4]);
                        return Long.valueOf(parts[4]);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            return 0L;
        }
    }

    private static class MessageSplitter implements FlatMapFunction<String, Tuple7<String,String,String, Integer, String,Integer,Integer>> {

        @Override
        public void flatMap(String s, Collector<Tuple7<String,String,String, Integer, String,Integer,Integer>> collector) throws Exception {
            if (s != null && s.contains(",")) {
                String[] strs = s.split(",");
                if(strs.length==7) {
                    collector.collect(new Tuple7<>(strs[0],strs[1],strs[2], Integer.parseInt(strs[3]), strs[4],Integer.parseInt(strs[5]),Integer.parseInt(strs[6])));
                }
            }
        }
    }

    public static class NullFilter implements FilterFunction<Tuple7<String,String,String, Integer, String,Integer,Integer>> {
        @Override
        public boolean filter(Tuple7<String,String,String, Integer, String,Integer,Integer> value) throws Exception {
            return value != null;
        }
    }

}
