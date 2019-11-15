package main;
/*
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.types.Row;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Properties;

/**
 * Flink Streaming Job to generate primary keys for json messages in kafka
 *
 */
public class StreamingJob {
	private static String datePattern = "yyyy-MM-dd";
	private static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(datePattern);

	public static void main(String[] args) throws Exception {
		ParameterTool parameterTool = ParameterTool.fromArgs(args);

		String jobName = parameterTool.get("job-name", "AggregationJob");
		String inputTopic = parameterTool.get("input-topic", "transaction_data");
		String outputTopic = parameterTool.get("output-topic", "test_topic_persist");
		String consumerGroup = parameterTool.get("group-id", "Group");
		String kafkaAddress = parameterTool.get("kafka-address", "localhost:9092"); // for running in eclipse use "localhost:9092", for flink cluster "kafka:29092"

		ObjectMapper objectMapper = new ObjectMapper();

		//get the execution environment
		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		//create a new kafka consumer
		Properties consumerProps = new Properties();
		consumerProps.setProperty("bootstrap.servers", kafkaAddress);
		consumerProps.setProperty("group.id", consumerGroup);
		FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>(inputTopic,
				new SimpleStringSchema(), consumerProps);

		//add the consumer to the environment as a data-source, to get a DataStream
		DataStream<String> dataStream = environment.addSource(flinkKafkaConsumer);

		// parse and add timestamps
		DataStream<ObjectNode> timestampedStream = dataStream
				// parse the json string
				.map((MapFunction<String, ObjectNode>) value -> (ObjectNode) objectMapper.readTree(value))
				// Assign a flink timestamp to each event
				.assignTimestampsAndWatermarks(new TimestampAndWatermarkAssigner());

		// Window and aggregate to months
		DataStream<Row> months = timestampedStream
				.keyBy((KeySelector<ObjectNode, String>) value -> value.get("payload").get("Order Date").asText().substring(0, 7))
				.window(new TumblingEventTimeMonthWindows())
				.aggregate(new MonthAggregator(datePattern));

		// Window and aggregate 12 months as input for the LSTM model
		DataStream<Row> modelInput = months
				.countWindowAll(17, 1)
				.aggregate(new InputAggregator(datePattern))
				.filter((FilterFunction<Row>) row -> row.getArity() == 6 && row.getField(5) != null
						&& ((Row)row.getField(5)).getArity() == 12 &&((Row)row.getField(5)).getField(11) != null)
				// LSTM prediction
				.map(new MapFunction<Row, Row>() {

					@Override
					public Row map(Row value) throws Exception {
						double[][] input = new double[6][12];
						for(int i=0; i<6; i++)
							for(int j=0; j<12; j++)
							input[i][j] = (Double)((Row)value.getField(i)).getField(j);
						//TODO model call
						return value;
					}
				});

		DataStream<String> outputStream = modelInput
				.map((MapFunction<Row, String>) row -> {
					String result = "\n\n";
					for(int i=0; i<12; i++) {
						result += " "+row.getField(i);
					}
					return result;
				});

		//create a new kafka producer
		Properties producerProps = new Properties();
		producerProps.setProperty("bootstrap.servers", kafkaAddress);
		FlinkKafkaProducer<String> flinkKafkaProducer = new FlinkKafkaProducer<>(outputTopic,
				new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()),
				producerProps, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);

		//add the producer to the dataStream as a sink
		outputStream.addSink(flinkKafkaProducer);

		environment.execute(jobName);
	}

	private static class TimestampAndWatermarkAssigner implements AssignerWithPunctuatedWatermarks<ObjectNode> {

		public TimestampAndWatermarkAssigner() {}

		@Override
		public long extractTimestamp(ObjectNode element, long previousElementTimestamp) {
			TemporalAccessor temporalAccessor = dateTimeFormatter.parse(element.get("payload").get("Order Date").asText());
			LocalDate localDate = LocalDate.from(temporalAccessor);
			Instant instant = localDate.atStartOfDay(ZoneId.systemDefault()).toInstant();
			return instant.toEpochMilli();
		}

		@Override
		public Watermark checkAndGetNextWatermark(ObjectNode lastElement, long extractedTimestamp) {
			return new Watermark(extractedTimestamp - Time.days(1).toMilliseconds());
		}
	}
}