import org.deeplearning4j.nn.modelimport.keras.KerasModelImport;
import org.deeplearning4j.nn.modelimport.keras.exceptions.InvalidKerasConfigurationException;
import org.deeplearning4j.nn.modelimport.keras.exceptions.UnsupportedKerasConfigurationException;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.io.ClassPathResource;

import java.io.IOException;
import java.io.Serializable;
import java.util.stream.IntStream;


public class LSTM implements Serializable {

    String model_path;
    MultiLayerNetwork model;
    DataTransform scaler;

    public LSTM() throws IOException, InvalidKerasConfigurationException, UnsupportedKerasConfigurationException {
        this.model_path = new ClassPathResource("my_model.h5").getFile().getPath();
        this.model = KerasModelImport.importKerasSequentialModelAndWeights(model_path);
        this.scaler = new DataTransform();

    }

    public double predict(double x){
        double[] dummy = {x};
        INDArray features = Nd4j.create(dummy);
        double y_pred = this.model.output(features).getDouble(0);

        double invers_pred = this.scaler.inverse_transform(y_pred);
        return invers_pred;
    }

}
