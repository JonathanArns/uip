//package main;
//
//import org.deeplearning4j.nn.api.TrainingConfig;
//import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
//import org.deeplearning4j.nn.modelimport.keras.KerasModelImport;
//import org.deeplearning4j.nn.modelimport.keras.exceptions.InvalidKerasConfigurationException;
//import org.deeplearning4j.nn.modelimport.keras.exceptions.UnsupportedKerasConfigurationException;
//import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
//import org.nd4j.linalg.api.ndarray.INDArray;
//import org.nd4j.linalg.factory.Nd4j;
//import org.nd4j.linalg.io.ClassPathResource;
//
//import java.io.File;
//import java.io.IOException;
//import java.io.Serializable;
//import java.util.List;
//
//
//public class LSTM implements Serializable {
//
//    String model_path;
//    MultiLayerNetwork model;
//    DataTransform scaler;
//
//    public LSTM() throws IOException, InvalidKerasConfigurationException, UnsupportedKerasConfigurationException {
////        this.model_path = new ClassPathResource("my_model.h5").getFile().getPath();
//        File f = new File("resources/my_model.h5");
//        this.model = KerasModelImport.importKerasSequentialModelAndWeights("/home/jonathan/workspaces/uip_workspace/uip-flink-jobs/LSTMJob/src/main/resources/LSTMmodel.h5", false);
//        this.scaler = new DataTransform();
//
//    }
//
//    public double[] predict(double[][] sales) {
//        int[] shape = new int[] {1, 11, 6};
//        double[] input = new double[6*11];
//        double[] diff;
//        for(int i=0; i<6; i++) {
//            diff = difference(sales[i]);
//            for(int j=0; j<11; j++)
//                input[i*11+j] = diff[j];
//        }
//
//
//        INDArray features = Nd4j.create(input, shape, 'f');
//        INDArray output = model.output(features);
//
//        return null;
//    }
//
//    private double[] difference(double[] sales) {
//        double[] result = new double[11];
//        for(int i=0; i<11; i++)
//            result[i] = sales[i] - sales[i+1];
//        return result;
//    }
//
//}
