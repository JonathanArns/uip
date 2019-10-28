
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
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;

import java.util.Properties;

/**
 * Flink Streaming Job to generate primary keys for json messages in kafka
 *
 */
public class StreamingJob {

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        String jobName = parameterTool.get("job-name", "KeyHashingJob");
        String inputTopic = parameterTool.get("input-topic", "test_topic");
        String outputTopic = parameterTool.get("output-topic", "test_topic_persist");
        String consumerGroup = parameterTool.get("group-id", "KeyHashingGroup");
        String kafkaAddress = parameterTool.get("kafka-address", "localhost:9092"); // for running in eclipse use "localhost:9092", for flink cluster "kafka:29092"

        ObjectMapper objectMapper = new ObjectMapper();

        //get the execution environment
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //create a new kafka consumer
        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", kafkaAddress);
        consumerProps.setProperty("group.id",consumerGroup);
        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>(inputTopic,
                new SimpleStringSchema(), consumerProps);

        //add the consumer to the environment as a data-source, to get a DataStream
        DataStream<String> dataStream = environment.addSource(flinkKafkaConsumer);

        // calculation
        DataStream<String> outputStream = dataStream
                // parse the json string
                .map((MapFunction<String, ObjectNode>) value -> (ObjectNode)objectMapper.readTree(value))
                // do the calculation and add the hash to the payload as well as its definition to the schema

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
}