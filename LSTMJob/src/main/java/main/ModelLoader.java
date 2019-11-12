package main;

import org.dmg.pmml.PMML;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.LoadingModelEvaluatorBuilder;
import org.jpmml.evaluator.visitors.DefaultVisitorBattery;
import org.jpmml.model.PMMLUtil;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class ModelLoader {
    public static Evaluator loadModelEvaluator(String pathToPMMLFile) throws JAXBException, SAXException, IOException {
        Evaluator evaluator = new LoadingModelEvaluatorBuilder()
                .setLocatable(false)
                .setVisitors(new DefaultVisitorBattery())
                //.setOutputFilter(OutputFilters.KEEP_FINAL_RESULTS)
                .load(new File(pathToPMMLFile))
                .build();

        // Perforing the self-check
        evaluator.verify();

        return evaluator;
    }


    public static PMML readPMML(File file) throws Exception {
        return readPMML(file, false);
    }

    static
    public PMML readPMML(File file, boolean acceptServiceJar) throws Exception {

        if(acceptServiceJar){

            if(isServiceJar(file, PMML.class)){
                URL url = (file.toURI()).toURL();

                return PMMLUtil.load(url);
            }
        }

        try(InputStream is = new FileInputStream(file)){
            return PMMLUtil.unmarshal(is);
        }
    }

    static
    private boolean isServiceJar(File file, Class<?> clazz){

        try(ZipFile zipFile = new ZipFile(file)){
            ZipEntry serviceZipEntry = zipFile.getEntry("META-INF/services/" + clazz.getName());

            return (serviceZipEntry != null);
        } catch(IOException ioe){
            return false;
        }
    }
}
