package main;

import jep.*;

public class JEPModel {
    public static void main(String[] args) {
        try(Interpreter interpreter = new SharedInterpreter()) {
            interpreter.exec("import numpy as np");
            interpreter.exec("import keras");
            interpreter.exec("model = keras.models.load_model('/home/jonathan/workspaces/uip_workspace/uip-flink-jobs/LSTMJob/src/main/resources/my_model.h5')");
            interpreter.exec("x = np.array([[[1]]])");
            Object result = interpreter.getValue("model.predict(x)");
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
