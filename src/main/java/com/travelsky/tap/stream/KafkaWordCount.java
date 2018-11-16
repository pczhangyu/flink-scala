package com.travelsky.tap.stream;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Properties;

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over some sample data
 *
 * <p>
 * This example shows how to:
 * <ul>
 * <li>write a simple Flink program.
 * <li>use Tuple data types.
 * <li>write and use user-defined functions.
 * </ul>
 *
 */
public class KafkaWordCount {

	//
	//	Program
	//

	public static void main(String[] args) throws Exception {

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(5000); // checkpoint every 5000 msecs
		
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "10.221.130.226:9093");
		// only required for Kafka 0.8
		properties.setProperty("zookeeper.connect", "10.221.130.226:2181,10.221.130.228:2181,10.221.130.230:2181/kafka-zkroot-test");
		properties.setProperty("group.id", String.format("%s-%s","zy",String.valueOf(System.currentTimeMillis())));
//		DataStreamSink<String> dataStream =env.
//				addSource(new FlinkKafkaConsumer010<>("test_tap", new SimpleStringSchema(), properties))
//				.print();
		// source
		DataStream<String> sourceStream = env.addSource(
				new FlinkKafkaConsumer010<String>("test_tap",
						new SimpleStringSchema(), properties));
		// Transformation，这里仅仅是过滤了null。
		DataStream<Tuple3<String, String, String>> messageStream = sourceStream
				.map(new InputMap())
				.filter(new NullFilter());
		messageStream.print();
//		//sink
		RandomAccessFile rad=new RandomAccessFile("E://demo.txt", "r");
		rad.seek(102);
		rad.readInt();
		rad.readShort();
//		messageStream.addSink(new ElasticSink());
		env.execute("KafkaWordCount");
		
	}

	//
	// 	User Functions
	//

	/**
	 * Implements the string tokenizer that splits sentences into words as a user-defined
	 * FlatMapFunction. The function takes a line (String) and splits it into
	 * multiple pairs in the form of "(word,1)" (Tuple2<String, Integer>).
	 */
	public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<String, Integer>(token, 1));
				}
			}
		}
	}

	// 过滤Null数据。
	public static class NullFilter implements FilterFunction<Tuple3<String, String, String>> {
		@Override
		public boolean filter(Tuple3<String, String, String> value) throws Exception {
			return value != null;
		}
	}

	// 对输入数据做map操作。
	public static class InputMap implements MapFunction<String, Tuple3<String, String, String>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple3<String, String, String> map(String line) throws Exception {
			// normalize and split the line
			String[] arr = line.toLowerCase().split(",");
			if (arr.length > 2) {
				return new Tuple3<>(arr[0], arr[1], arr[2]);
			}
			return null;
		}
	}
}
