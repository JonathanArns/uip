package main;

import jep.*;

public class JEPModel {
    public static void main(String[] args) {
        try(Interpreter interpreter = new SharedInterpreter()) {
            Double[] data = {};
            NDArray<Double[]> input = new NDArray<>(data, 1, 11, 6);

            interpreter.exec("import numpy as np");
            interpreter.exec("import keras");
            interpreter.exec("from sklearn.externals import joblib");
            interpreter.exec("scaler = joblib.load('src/main/resources/scaler_lstm.save')");
            interpreter.exec("model = keras.models.load_model('src/main/resources/my_model.h5')");

            interpreter.set("input", input);
            interpreter.exec("features = scaler.transform(input)");
            Object result = interpreter.getValue("model.predict(features)");
            System.out.println(result);
            //TODO remove dl4j stuff
            //TODO minmax
            //TODO get input from java to python
        } catch (JepException e) {
            e.printStackTrace();
        } finally {

        }
    }
}
