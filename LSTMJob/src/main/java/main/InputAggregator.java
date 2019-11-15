package main;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.types.Row;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
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
        accumulator.put(LocalDate.from(dateTimeFormatter.parse((String) value.getField(0))), (Double) value.getField(1));
        return accumulator;
    }

    @Override
    public Row getResult(LinkedHashMap<LocalDate, Double> accumulator) {
        Set<LocalDate> tmp = accumulator.keySet();
        ArrayList<LocalDate> dates = new ArrayList<>();
        dates.addAll(tmp);
        dates.sort(LocalDate::compareTo);
        Row row;
        Row result = new Row(6);
        for(int j = 0; j < dates.size() && j < 6; j++) {
            row = new Row(12);
            for (int i = 0+j; i < dates.size() && i < 12+j; i++)
                row.setField(i-j, accumulator.get(dates.get(dates.size() - (i + 1))));
            result.setField(j, row);
        }
        return result;
    }

    @Override
    public LinkedHashMap<LocalDate, Double> merge(LinkedHashMap<LocalDate, Double> a, LinkedHashMap<LocalDate, Double> b) {
        a.putAll(b);
        return a;
    }
}