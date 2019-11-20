package main;

import jep.Interpreter;
import jep.JepException;
import jep.NDArray;
import jep.SharedInterpreter;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class WindowProcessor extends ProcessAllWindowFunction<Row, double[], GlobalWindow> {
    private static DateTimeFormatter dateTimeFormatter;
    private static Interpreter interpreter = null;

//    static {
//        try {
//            interpreter = new SharedInterpreter();
//            interpreter.exec("import numpy as np");
//            interpreter.exec("import keras");
////        interpreter.exec("import keras.backend.tensorflow_backend as tb");
//            interpreter.exec("keras.backend.tensorflow_backend._SYMBOLIC_SCOPE.value = True");
//            interpreter.exec("from sklearn.externals import joblib");
//            interpreter.exec("feature_scaler = joblib.load('src/main/resources/scaler_feature.save')");
////        interpreter.exec("inverse_scaler = joblib.load('src/main/resources/inverse_scaler.save')");
//            interpreter.exec("with open('src/main/resources/model_architecture.json','r') as f : " +
//                    "model = keras.models.model_from_json(f.read())");
//            interpreter.exec("model.load_weights('src/main/resources/lstm_weights.h5')");
//        } catch (JepException e) {
//            e.printStackTrace();
//        }
//    }

    public WindowProcessor(String datePattern) {
        dateTimeFormatter = DateTimeFormatter.ofPattern(datePattern);
    }

    @Override
    public void process(Context context, Iterable<Row> elements, Collector<double[]> out) throws Exception {
        if(interpreter == null) {
            try {
                interpreter = new SharedInterpreter();
                interpreter.exec("import numpy as np");
                interpreter.exec("import keras");
                //        interpreter.exec("import keras.backend.tensorflow_backend as tb");
//                interpreter.exec("keras.backend.tensorflow_backend._SYMBOLIC_SCOPE.value = True");
                interpreter.exec("from sklearn.externals import joblib");
                interpreter.exec("feature_scaler = joblib.load('src/main/resources/scaler_feature.save')");
                //        interpreter.exec("inverse_scaler = joblib.load('src/main/resources/inverse_scaler.save')");
                interpreter.exec("with open('src/main/resources/model_architecture.json','r') as f : " +
                        "model = keras.models.model_from_json(f.read())");
                interpreter.exec("model.load_weights('src/main/resources/lstm_weights.h5')");
            } catch (JepException e) {
                e.printStackTrace();
            }
        }


        Iterator<Row> iterator = elements.iterator();
        HashMap<LocalDate, Double> window = new HashMap<>();
        while(iterator.hasNext()) {
            Row row = iterator.next();
            window.put(LocalDate.from(dateTimeFormatter.parse((String) row.getField(0))), (Double) row.getField(1));
        }
        if(window.size() < 17) {
            out.close();
            return;
        }

        Collection<LocalDate> tmp = window.keySet();
        ArrayList<LocalDate> dates = new ArrayList<>();
        dates.addAll(tmp);
        dates.sort(LocalDate::compareTo);

        HashMap<LocalDate, Double> differenced = new HashMap<>();
        for(int i = 0; i < dates.size()-1; i++)
            differenced.put(dates.get(i), window.get(dates.get(i+1)) - window.get(dates.get(i)));
        dates.remove(dates.size()-1); //TODO ist das wirklich richtig????

        double[] data = new double[6*11];
        for(int i=0; i<6; i++) {
            for (int j = i; j < 11 + i && j < dates.size(); j++)
                data[11 * i + j - i] = differenced.get(dates.get(j));
        }

        System.out.println("\n------------------Months in order----------------");
        for(LocalDate date : dates) {
            System.out.println(date.toString());
        }


        NDArray<double[]> ndInput = new NDArray<>(data, 6, 11);
        interpreter.set("input", ndInput);
        interpreter.exec("features = np.concatenate([input,[[0],[0],[0],[0],[0],[0]]], axis=1)");
        interpreter.exec("features = feature_scaler.transform(features)");
        interpreter.exec("features = np.delete(features, -1, axis=1)");
        interpreter.exec("features = features.reshape(6,1,11)");
        interpreter.exec("prediction = model.predict(features, batch_size=1)");
        interpreter.exec("prediction = prediction.reshape(prediction.shape[0], 1, prediction.shape[1])");
        interpreter.exec("dataset = []");
        interpreter.exec("for i in range(0,len(prediction)): " +
//                "dataset.append(np.concatenate([prediction[i], [[0,0,0,0,0,0,0,0,0,0,0]]], axis=1))");
                "dataset.append(np.concatenate([prediction[i], features[i]], axis=1))");
        interpreter.exec("dataset = np.array(dataset)");
        interpreter.exec("dataset = dataset.reshape(dataset.shape[0], dataset.shape[2])");
        interpreter.exec("output = feature_scaler.inverse_transform(dataset)");
//        interpreter.exec("print(output)");
        NDArray<double[]> output = (NDArray<double[]>)interpreter.getValue("output");
//        interpreter.close();
        out.collect(output.getData());
        out.close();
    }
}
