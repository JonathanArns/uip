import java.io.Serializable;

public class DataTransform implements Serializable {

    public static double fit_transform(double sales){
        return Math.tanh(sales);
    }

    public static double inverse_transform(double inverse_sales){
        return (Math.log(1+inverse_sales)-Math.log(1-inverse_sales))/2;
    }


}
