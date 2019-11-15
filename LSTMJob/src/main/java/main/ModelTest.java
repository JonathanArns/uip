//package main;
//
//import org.deeplearning4j.nn.api.TrainingConfig;
//import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
//import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
//import org.deeplearning4j.nn.modelimport.keras.exceptions.InvalidKerasConfigurationException;
//import org.deeplearning4j.nn.modelimport.keras.exceptions.UnsupportedKerasConfigurationException;
//import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
//import org.nd4j.linalg.api.ndarray.INDArray;
//import org.nd4j.linalg.factory.Nd4j;
//
//import java.io.IOException;
//import java.util.List;
//
//public class ModelTest {
//
//    public ModelTest() throws UnsupportedKerasConfigurationException, IOException, InvalidKerasConfigurationException {
//    }
//
//    public static void main(String[] args) throws InvalidKerasConfigurationException, IOException, UnsupportedKerasConfigurationException {
//        ModelTest m = new ModelTest();
//        MultiLayerNetwork model = new LSTM().model;
//
//        double[][][] input = new double[][][] {{
//                {0.62768387, 0.09062461, -0.30739122, 0.9228323, -0.13020111,
//                        -0.78076888, -0.02864555, 1.11038965, -0.36652838, 0.15390567, 1.02608347},
//
//                {-0.70591767, 0.62768387,  0.09062461, -0.30739122,  0.9228323,
//                        -0.13020111, -0.78076888,  0.02496819,  1.11038965, -0.36652838, 0.1689546},
//
//                {1.33934461, -0.70591767,  0.62768387,  0.09062461, -0.30739122,
//                        0.9228323,  -0.13020111, -0.76866845,  0.02496819,  1.11038965, -0.35826682},
//
//                {-0.15649743,  1.33934461, -0.70591767,  0.62768387, 0.09062461,
//                        -0.30739122,  0.9228323,  -0.08219271, -0.76866845,  0.02496819, 1.13791279},
//
//                { 0.07871758, -0.15649743,  1.33934461, -0.70591767,  0.62768387,
//                        0.09062461, -0.30739122,  1.0289627,  -0.08219271, -0.76866845, 0.03833555},
//
//                { 0.91238719,  0.07871758, -0.15649743,  1.33934461, -0.70591767,
//                        0.62768387,  0.09062461, -0.2691628,   1.0289627,  -0.08219271, -0.76565149}}};
//
//        double[] input2 = new double[]
//                {0.62768387, 0.09062461, -0.30739122, 0.9228323, -0.13020111, -0.78076888, -0.02864555, 1.11038965, -0.36652838, 0.15390567, 1.02608347,
//
//                -0.70591767, 0.62768387,  0.09062461, -0.30739122,  0.9228323, -0.13020111, -0.78076888,  0.02496819,  1.11038965, -0.36652838, 0.1689546,
//
//                1.33934461, -0.70591767,  0.62768387,  0.09062461, -0.30739122, 0.9228323,  -0.13020111, -0.76866845,  0.02496819,  1.11038965, -0.35826682,
//
//                -0.15649743,  1.33934461, -0.70591767,  0.62768387, 0.09062461, -0.30739122,  0.9228323,  -0.08219271, -0.76866845,  0.02496819, 1.13791279,
//
//                 0.07871758, -0.15649743,  1.33934461, -0.70591767,  0.62768387, 0.09062461, -0.30739122,  1.0289627,  -0.08219271, -0.76866845, 0.03833555,
//
//                0.91238719,  0.07871758, -0.15649743,  1.33934461, -0.70591767, 0.62768387,  0.09062461, -0.2691628,   1.0289627,  -0.08219271, -0.76565149};
//
//        MultiLayerConfiguration configuration = model.getLayerWiseConfigurations();
//        int[] shape = new int[] {1,11,6};
//        INDArray features = Nd4j.create(input2, shape, 'f');
//        List<INDArray> output = model.feedForward(features);
//        INDArray output2 = model.rnnTimeStep(features);
//        System.out.println("");
//    }
//}
