package main;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * The accumulator is used to keep a running sum and a count. The {@code getResult} method
 * computes the average.
 */
public class MonthAggregator implements AggregateFunction<ObjectNode, Tuple2<Double, ArrayList<Long>>, Tuple2<String, Double>> {
    private static ObjectMapper objectMapper = new ObjectMapper();
    private static DateTimeFormatter dateTimeFormatter;

    public MonthAggregator(String datePattern) {
        dateTimeFormatter = DateTimeFormatter.ofPattern(datePattern);
    }

    @Override
    public Tuple2<Double, ArrayList<Long>> createAccumulator() {
        return new Tuple2<>(0.0, new ArrayList<>());
    }

    @Override
    public Tuple2<Double, ArrayList<Long>> add(ObjectNode value, Tuple2<Double, ArrayList<Long>> accumulator) {
        accumulator.f1.add(getTimestamp(value));
        return new Tuple2<>(
                accumulator.f0 + Double.parseDouble(value.get("payload").get("Sales").asText()),
                accumulator.f1
        );
    }

    @Override
    public Tuple2<String, Double> getResult(Tuple2<Double, ArrayList<Long>> accumulator) {
        // Count the dates and use most common, to eliminate early or late elements' dates
        HashMap<Long, Integer> counts = new HashMap<>();
        accumulator.f1.forEach(timestamp -> {
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
        String date = dateTime.format(dateTimeFormatter).substring(0, 8) + "01";

        return new Tuple2<String, Double>(date, accumulator.f0);
    }

    @Override
    public Tuple2<Double, ArrayList<Long>> merge(Tuple2<Double, ArrayList<Long>> a, Tuple2<Double, ArrayList<Long>> b) {
        a.f1.addAll(b.f1);
        return new Tuple2<>(a.f0+b.f0, a.f1);
    }

    private long getTimestamp(ObjectNode element) {
        TemporalAccessor temporalAccessor = dateTimeFormatter.parse(element.get("payload").get("Order Date").asText());
        LocalDate localDate = LocalDate.from(temporalAccessor);
        Instant instant = localDate.atStartOfDay(ZoneId.systemDefault()).toInstant();
        return instant.toEpochMilli();
    }
}