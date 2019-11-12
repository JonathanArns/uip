package main;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.types.Row;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Set;

/**
 * The accumulator is used to keep a running sum and a count. The {@code getResult} method
 * computes the average.
 */
public class InputAggregator implements AggregateFunction<Row, LinkedHashMap<LocalDate, Double>, Row> {
    private static DateTimeFormatter dateTimeFormatter;

    public InputAggregator(String datePattern) {
        dateTimeFormatter = DateTimeFormatter.ofPattern(datePattern);
    }

    @Override
    public LinkedHashMap<LocalDate, Double> createAccumulator() {
        return new LinkedHashMap<>();
    }

    @Override
    public LinkedHashMap<LocalDate, Double> add(Row value, LinkedHashMap<LocalDate, Double> accumulator) {
        accumulator.put(LocalDate.from(dateTimeFormatter.parse((String)value.getField(0))), (Double)value.getField(1));
        return accumulator;
    }

    @Override
    public Row getResult(LinkedHashMap<LocalDate, Double> accumulator) {
        Set<LocalDate> tmp = accumulator.keySet();
        ArrayList<LocalDate> dates= new ArrayList<>();
        dates.addAll(tmp);
        dates.sort(LocalDate::compareTo);
        Row row = new Row(12);
        for(int i=0; i<12; i++)
            row.setField(i, accumulator.get(dates.get(i)));
        return row;
    }

    @Override
    public LinkedHashMap<LocalDate, Double> merge(LinkedHashMap<LocalDate, Double> a, LinkedHashMap<LocalDate, Double> b) {
        a.putAll(b);
        return a;
    }

//    public Tuple12<Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double> add(Row value, Tuple12<Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double> accumulator) {
//        return null;
//    }
//
//    public Row getResult(Tuple12<Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double> accumulator) {
//        return null;
//    }
//
//    public Tuple12<Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double> merge(Tuple12<Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double> a, Tuple12<Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double, Double> b) {
//        return null;
//    }
//
//    public Tuple2<Double, ArrayList<Long>> add(ObjectNode value, Tuple2<Double, ArrayList<Long>> accumulator) {
//        accumulator.f1.add(getTimestamp(value));
//        return new Tuple2<>(
//                accumulator.f0 + Double.parseDouble(value.get("payload").get("Sales").asText()),
//                accumulator.f1
//        );
//    }
//
//    public Row getResult(Tuple2<Double, ArrayList<Long>> accumulator) {
//        // Count the dates and use most common, to eliminate early or late elements' dates
//        HashMap<Long, Integer> counts = new HashMap<>();
//        accumulator.f1.forEach(timestamp -> {
//            if(counts.containsKey(timestamp))
//                counts.put(timestamp, counts.get(timestamp)+1);
//            else
//                counts.put(timestamp, 1);
//        });
//        long timestamp = 0; int count = 0;
//        for(Long ts : counts.keySet()) {
//            if(counts.get(ts) > count) {
//                timestamp = ts;
//                count = counts.get(ts);
//            }
//        }
//        // Format timestamp to date String
//        Instant instant = Instant.ofEpochMilli(timestamp);
//        LocalDateTime dateTime = instant.atZone(ZoneId.systemDefault()).toLocalDateTime();
//        String date = dateTime.format(dateTimeFormatter).substring(0, 8) + "01";
//
//        // Build the Json object to return
//        ObjectNode result = objectMapper.createObjectNode();
//        ObjectNode schema = objectMapper.createObjectNode();
//        ObjectNode payload = objectMapper.createObjectNode();
//        ArrayNode fieldSchemas = schema.put("type", "struct").putArray("fields");
//        ObjectNode dateSchema = objectMapper.createObjectNode().put("field", "Order Date").put("type", "string").put("optional", "false");
//        ObjectNode salesSchema = objectMapper.createObjectNode().put("field", "Sales").put("type", "int64").put("optional", "false");;
//        fieldSchemas.add(dateSchema).add(salesSchema);
//        payload.put("Order Date", date)
//                .put("Sales", accumulator.f0);
//        result.set("schema", schema);
//        result.set("payload", payload);
//        Row row = new Row(2);
//        row.setField(0, date);
//        row.setField(1, accumulator.f0);
//        return row;
//    }
//
//    public Tuple2<Double, ArrayList<Long>> merge(Tuple2<Double, ArrayList<Long>> a, Tuple2<Double, ArrayList<Long>> b) {
//        a.f1.addAll(b.f1);
//        return new Tuple2<>(a.f0+b.f0, a.f1);
//    }
//
//    private long getTimestamp(ObjectNode element) {
//        TemporalAccessor temporalAccessor = dateTimeFormatter.parse(element.get("payload").get("Order Date").asText());
//        LocalDate localDate = LocalDate.from(temporalAccessor);
//        Instant instant = localDate.atStartOfDay(ZoneId.systemDefault()).toInstant();
//        return instant.toEpochMilli();
//    }
}