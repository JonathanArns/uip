package main;

import jep.*;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Set;

public class JEPModel extends Thread {
    private LinkedHashMap<LocalDate, Double> input;
    private double[] output = new double[] {};

    public JEPModel(LinkedHashMap<LocalDate, Double> input) throws JepException {
        this.input = input;
    }

    @Override
    public void run() {
        try {
            output = predict(input);
        } catch (JepException e) {
            e.printStackTrace();
        }
    }

    public static double[] predict(LinkedHashMap<LocalDate, Double> input) throws JepException {
//        Interpreter interpreter = new SubInterpreter(new JepConfig().addSharedModules("sklearn", "keras", "numpy", "google.protobuf", "h5py"));
        Interpreter interpreter = new SharedInterpreter();
//        System.out.println("\n\nnew SubInterpreter()\n\n");

        interpreter.exec("import numpy as np");
        interpreter.exec("import keras");

//        interpreter.exec("import keras.backend.tensorflow_backend as tb");
        interpreter.exec("keras.backend.tensorflow_backend._SYMBOLIC_SCOPE.value = True");

        interpreter.exec("from sklearn.externals import joblib");
        interpreter.exec("feature_scaler = joblib.load('src/main/resources/scaler_feature.save')");
//        interpreter.exec("inverse_scaler = joblib.load('src/main/resources/inverse_scaler.save')");

        interpreter.exec("with open('src/main/resources/model_architecture.json','r') as f : " +
                "model = keras.models.model_from_json(f.read())");
        interpreter.exec("model.load_weights('src/main/resources/lstm_weights.h5')");

        Set<LocalDate> keySet = input.keySet();
        ArrayList<LocalDate> dates = new ArrayList<>();
        dates.addAll(keySet);
        dates.sort(LocalDate::compareTo);
//        System.out.println("Expected: dates from early to late"); //debuggin TODO remove
//        String dateString = "";
//        for(int i=0; i<dates.size(); i++)
//            dateString += "\n"+dates.get(i).toString();
//        System.out.println(dateString);

        HashMap<LocalDate, Double> differenced = new HashMap<>();
        for(int i = 0; i < dates.size()-1; i++)
            differenced.put(dates.get(i), input.get(dates.get(i+1)) - input.get(dates.get(i)));
        dates.remove(dates.size()-1);
        //differenced is now hashmap of diffed values with dates
//        System.out.println("Expected: input sales in date order"); //debuggin TODO remove
//        for(LocalDate date : dates)
//            System.out.println(input.get(date));
//        System.out.println("Expected: differenced sales in date order"); //debuggin TODO remove
//        for(LocalDate date : dates)
//            System.out.println(differenced.get(date));

        double[] data = new double[6*11];
        for(int i=0; i<6; i++)
            for(int j=i; j<11+i && j<dates.size(); j++)
                data[11*i+j-i] = differenced.get(dates.get(j));

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
        interpreter.close();
        return output.getData();
//        return new double[] {0,0,0,0,0,0};
    }

    public double[] getOutput() {
        return output;
    }
}
