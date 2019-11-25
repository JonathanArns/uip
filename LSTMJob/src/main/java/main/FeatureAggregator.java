package main;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;

/**
 * A window aggregator class that is used to collect the data necessary to build the features
 * of the LSTM Sales prediction model.
 *
 * The result is json in the format:
 * {data: [{"date":"2014-01-01","sales":3278.23}, {"date":"2014-02-01","sales":57322.432}, ... ]}
 */
public class FeatureAggregator implements AggregateFunction<Tuple2<String, Double>, LinkedHashMap<LocalDate, Double>, ObjectNode> {
    private DateTimeFormatter dateTimeFormatter = null;
    private String datePattern;

    public FeatureAggregator(String datePattern) {
        this.datePattern = datePattern;
    }

    @Override
    public LinkedHashMap<LocalDate, Double> createAccumulator() {
        return new LinkedHashMap<>();
    }

    @Override
    public LinkedHashMap<LocalDate, Double> add(Tuple2<String, Double> value, LinkedHashMap<LocalDate, Double> accumulator) {
        if(dateTimeFormatter == null)
            dateTimeFormatter = DateTimeFormatter.ofPattern(datePattern);

        accumulator.put(LocalDate.from(dateTimeFormatter.parse(value.f0)), value.f1);
        return accumulator;
    }

    @Override
    public ObjectNode getResult(LinkedHashMap<LocalDate, Double> accumulator) {
        if(accumulator.size() < 17)
            return null;
        ObjectMapper objectMapper = new ObjectMapper();
        ArrayNode data = objectMapper.createArrayNode();
        for(LocalDate key : accumulator.keySet()) {
            data.add(objectMapper.createObjectNode()
            .put("date", key.format(dateTimeFormatter))
            .put("sales", accumulator.get(key)));
        }
        ObjectNode result = objectMapper.createObjectNode().set("data", data);

        return result;
    }

    @Override
    public LinkedHashMap<LocalDate, Double> merge(LinkedHashMap<LocalDate, Double> a, LinkedHashMap<LocalDate, Double> b) {
        a.putAll(b);
        return a;
    }
}