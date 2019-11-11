
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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;

import java.io.Serializable;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.*;

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
        consumerProps.setProperty("group.id",consumerGroup);
        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>(inputTopic,
                new SimpleStringSchema(), consumerProps);

        //add the consumer to the environment as a data-source, to get a DataStream
        DataStream<String> dataStream = environment.addSource(flinkKafkaConsumer);

        // parse and add timestamps
        DataStream<ObjectNode> timestampedStream = dataStream
                // parse the json string
                .map((MapFunction<String, ObjectNode>) value -> (ObjectNode)objectMapper.readTree(value))
                // Assign a flink timestamp to each event
                .assignTimestampsAndWatermarks(new TimestampAndWatermarkAssigner());

        // Window and aggregate to days
        DataStream<ObjectNode> days = timestampedStream
                .windowAll(new TumblingEventTimeMonthWindows())
                .aggregate(new DayAggregator());

        DataStream<String> outputStream = days
                // do the calculation
                .map((MapFunction<ObjectNode, ObjectNode>) value -> {
                    //TODO ML API call and return result with a new json schema
                    return value;
                })
                // convert to string again
                .map((MapFunction<ObjectNode, String>) value -> objectMapper.writeValueAsString(value));

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
        private long myPreviousElementTimestamp = Long.MIN_VALUE;
        private long latestWatermark = Long.MIN_VALUE;
        private LinkedList<Long> timestamps = new LinkedList<>();

        public TimestampAndWatermarkAssigner() {
            timestamps.add(Long.MIN_VALUE);
        }

        @Override
        public long extractTimestamp(ObjectNode element, long previousElementTimestamp) {
            this.myPreviousElementTimestamp = previousElementTimestamp;
            long timestamp = getTimestamp(element);
            timestamps.add(timestamp);
            return timestamp;
        }

        @Override
        public Watermark checkAndGetNextWatermark(ObjectNode lastElement, long extractedTimestamp) {
            while(timestamps.size() > 2) timestamps.pop();
            return timestamps.get(timestamps.size()-2) < timestamps.get(timestamps.size()-1) ? new Watermark(timestamps.get(0)) : null;
        }

        private long getTimestamp(ObjectNode element) {
            TemporalAccessor temporalAccessor = dateTimeFormatter.parse(element.get("payload").get("Order Date").asText());
            LocalDate localDate = LocalDate.from(temporalAccessor);
            Instant instant = localDate.atStartOfDay(ZoneId.systemDefault()).toInstant();
            return instant.toEpochMilli();
        }
    }

    /**
     * The accumulator is used to keep a running sum and a count. The {@code getResult} method
     * computes the average.
     */
    private static class DayAggregator
            implements AggregateFunction<ObjectNode, Tuple5<Double, Double, Double, Double, ArrayList<Long>>, ObjectNode> {
        ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public Tuple5<Double, Double, Double, Double, ArrayList<Long>> createAccumulator() {
            return new Tuple5<>(0.0, 0.0, 0.0, 0.0, new ArrayList<>());
        }

        @Override
        public Tuple5<Double, Double, Double, Double, ArrayList<Long>> add(ObjectNode value, Tuple5<Double, Double, Double, Double, ArrayList<Long>> accumulator) {
            accumulator.f4.add(getTimestamp(value));
            return new Tuple5<>(
                    accumulator.f0 + Double.parseDouble(value.get("payload").get("Sales").asText()),
                    accumulator.f1 + Double.parseDouble(value.get("payload").get("Quantity").asText()),
                    accumulator.f2 + Double.parseDouble(value.get("payload").get("Discount").asText()),
                    accumulator.f3 + Double.parseDouble(value.get("payload").get("Profit").asText()),
                    accumulator.f4
            ); //TODO discount ist prozent
        }

        @Override
        public ObjectNode getResult(Tuple5<Double, Double, Double, Double, ArrayList<Long>> accumulator) {
            // Count the dates and use most common, to eliminate early or late elements' dates
            HashMap<Long, Integer> counts = new HashMap<>();
            accumulator.f4.forEach(timestamp -> {
                if(counts.containsKey(timestamp))
                    counts.put(timestamp, counts.get(timestamp)+1);
                else
                    counts.put(timestamp, 1);
            });
            long timestamp = 0; int count = 0;
            for(Long ts : counts.keySet()) {
                if(counts.get(ts) > count) {
                    timestamp = ts;
                    count = counts.get(ts);
                }
            }
            // Format timestamp to date String
            Instant instant = Instant.ofEpochMilli(timestamp);
            LocalDateTime dateTime = instant.atZone(ZoneId.systemDefault()).toLocalDateTime();
            String date = dateTime.format(dateTimeFormatter);

            // Build the Json object to return
            ObjectNode result = objectMapper.createObjectNode();
            ObjectNode schema = objectMapper.createObjectNode();
            ObjectNode payload = objectMapper.createObjectNode();
            ArrayNode fieldSchemas = schema.put("type", "struct").putArray("fields");
            ObjectNode dateSchema = objectMapper.createObjectNode().put("field", "Order Date").put("type", "string").put("optional", "false");
            ObjectNode salesSchema = objectMapper.createObjectNode().put("field", "Sales").put("type", "int64").put("optional", "false");;
            ObjectNode quantitySchema = objectMapper.createObjectNode().put("field", "Quantity").put("type", "in64").put("optional", "false");;
            ObjectNode discountSchema = objectMapper.createObjectNode().put("field", "Discount").put("type", "in64").put("optional", "false");;
            ObjectNode profitSchema = objectMapper.createObjectNode().put("field", "Profit").put("type", "in64").put("optional", "false");;
            fieldSchemas.add(dateSchema).add(salesSchema).add(quantitySchema).add(discountSchema).add(profitSchema);
            payload.put("Order Date", date)
                    .put("Sales", accumulator.f0)
                    .put("Quantity", accumulator.f1)
                    .put("Discount", accumulator.f2)
                    .put("Profit", accumulator.f3);
            result.set("schema", schema);
            result.set("payload", payload);
            return result;
        }

        @Override
        public Tuple5<Double, Double, Double, Double, ArrayList<Long>> merge(Tuple5<Double, Double, Double, Double, ArrayList<Long>> a, Tuple5<Double, Double, Double, Double, ArrayList<Long>> b) {
            a.f4.addAll(b.f4);
            return new Tuple5<>(a.f0+b.f0, a.f1+b.f1, a.f2+b.f2, a.f3+b.f3, a.f4);
        }

        private long getTimestamp(ObjectNode element) {
            TemporalAccessor temporalAccessor = dateTimeFormatter.parse(element.get("payload").get("Order Date").asText());
            LocalDate localDate = LocalDate.from(temporalAccessor);
            Instant instant = localDate.atStartOfDay(ZoneId.systemDefault()).toInstant();
            return instant.toEpochMilli();
        }
    }

    private static class MonthAggregator
            implements AggregateFunction<ObjectNode, Tuple5<Double, Double, Double, Double, ArrayList<Long>>, ObjectNode> {
        ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public Tuple5<Double, Double, Double, Double, ArrayList<Long>> createAccumulator() {
            return new Tuple5<>(0.0, 0.0, 0.0, 0.0, new ArrayList<>());
        }

        @Override
        public Tuple5<Double, Double, Double, Double, ArrayList<Long>> add(ObjectNode value, Tuple5<Double, Double, Double, Double, ArrayList<Long>> accumulator) {
            accumulator.f4.add(getTimestamp(value));
            return new Tuple5<>(
                    accumulator.f0 + Double.parseDouble(value.get("payload").get("Sales").asText()),
                    accumulator.f1 + Double.parseDouble(value.get("payload").get("Quantity").asText()),
                    accumulator.f2 + Double.parseDouble(value.get("payload").get("Discount").asText()),
                    accumulator.f3 + Double.parseDouble(value.get("payload").get("Profit").asText()),
                    accumulator.f4
            ); //TODO discount ist prozent
        }

        @Override
        public ObjectNode getResult(Tuple5<Double, Double, Double, Double, ArrayList<Long>> accumulator) {
            // Count the dates and use most common, to eliminate early or late elements' dates
            HashMap<Long, Integer> counts = new HashMap<>();
            accumulator.f4.forEach(timestamp -> {
                if(counts.containsKey(timestamp))
                    counts.put(timestamp, counts.get(timestamp)+1);
                else
                    counts.put(timestamp, 1);
            });
            long timestamp = 0; int count = 0;
            for(Long ts : counts.keySet()) {
                if(counts.get(ts) > count) {
                    timestamp = ts;
                    count = counts.get(ts);
                }
            }
            // Format timestamp to date String
            Instant instant = Instant.ofEpochMilli(timestamp);
            LocalDateTime dateTime = instant.atZone(ZoneId.systemDefault()).toLocalDateTime();
            String date = dateTime.format(dateTimeFormatter);

            // Build the Json object to return
            ObjectNode result = objectMapper.createObjectNode();
            ObjectNode schema = objectMapper.createObjectNode();
            ObjectNode payload = objectMapper.createObjectNode();
            ArrayNode fieldSchemas = schema.put("type", "struct").putArray("fields");
            ObjectNode dateSchema = objectMapper.createObjectNode().put("field", "Order Date").put("type", "string").put("optional", "false");
            ObjectNode salesSchema = objectMapper.createObjectNode().put("field", "Sales").put("type", "int64").put("optional", "false");;
            ObjectNode quantitySchema = objectMapper.createObjectNode().put("field", "Quantity").put("type", "in64").put("optional", "false");;
            ObjectNode discountSchema = objectMapper.createObjectNode().put("field", "Discount").put("type", "in64").put("optional", "false");;
            ObjectNode profitSchema = objectMapper.createObjectNode().put("field", "Profit").put("type", "in64").put("optional", "false");;
            fieldSchemas.add(dateSchema).add(salesSchema).add(quantitySchema).add(discountSchema).add(profitSchema);
            payload.put("Order Date", date)
                    .put("Sales", accumulator.f0)
                    .put("Quantity", accumulator.f1)
                    .put("Discount", accumulator.f2)
                    .put("Profit", accumulator.f3);
            result.set("schema", schema);
            result.set("payload", payload);
            return result;
        }

        @Override
        public Tuple5<Double, Double, Double, Double, ArrayList<Long>> merge(Tuple5<Double, Double, Double, Double, ArrayList<Long>> a, Tuple5<Double, Double, Double, Double, ArrayList<Long>> b) {
            a.f4.addAll(b.f4);
            return new Tuple5<>(a.f0+b.f0, a.f1+b.f1, a.f2+b.f2, a.f3+b.f3, a.f4);
        }

        private long getTimestamp(ObjectNode element) {
            TemporalAccessor temporalAccessor = dateTimeFormatter.parse(element.get("payload").get("Order Date").asText());
            LocalDate localDate = LocalDate.from(temporalAccessor);
            Instant instant = localDate.atStartOfDay(ZoneId.systemDefault()).toInstant();
            return instant.toEpochMilli();
        }
    }
}