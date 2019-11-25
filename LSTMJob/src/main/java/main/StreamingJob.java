package main;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
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

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.*;

/**
 * Flink Streaming Job to collect the data necessary to build the features of the LSTM sales prediction model.
 *
 * This Job aggregates sliding windows of 17 months of data and writes these windows back to kafka
 * in json format like:
 * {data: [{"date":"2014-01-01","sales":3278.23}, {"date":"2014-02-01","sales":57322.432}, ... ]}
 */
public class StreamingJob {
	private static String datePattern = "yyyy-MM-dd";
	private static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(datePattern);


	public static void main(String[] args) throws Exception {
		ParameterTool parameterTool = ParameterTool.fromArgs(args);

		String jobName = parameterTool.get("job-name", "AggregationJob");
		String inputTopic = parameterTool.get("input-topic", "transaction_data");
		String outputTopic = parameterTool.get("output-topic", "test");
		String consumerGroup = parameterTool.get("group-id", "Group");
		String kafkaAddress = parameterTool.get("kafka-address", "kafka:29092"); // for running in eclipse use "localhost:9092", for flink cluster "kafka:29092"

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

		// parse json and assign a flink event-time timestamp to each element
		DataStream<ObjectNode> timestampedStream = dataStream
				.map((MapFunction<String, ObjectNode>) value -> (ObjectNode) objectMapper.readTree(value))
				.assignTimestampsAndWatermarks(new TimestampAndWatermarkAssigner());

		// Window and aggregate to months
		DataStream<Tuple2<String, Double>> months = timestampedStream
				.keyBy((KeySelector<ObjectNode, String>) value -> value.get("payload").get("Order Date").asText().substring(0, 7))
				.window(new TumblingEventTimeMonthWindows())
				.aggregate(new MonthAggregator(datePattern));

		// Window and aggregate 17 months as json input for the LSTM model
		DataStream<ObjectNode> featureStream = months
				.countWindowAll(17, 1)
				.aggregate(new FeatureAggregator(datePattern))
				.filter((FilterFunction<ObjectNode>) value -> value != null);

		// transform json objects to String to write to kafka
		DataStream<String> outputStream = featureStream
				.map(value -> objectMapper.writeValueAsString(value));

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