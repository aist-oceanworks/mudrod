package esiptestbed.mudrod.ssearch.ranking;

import scala.Tuple2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;

import esiptestbed.mudrod.main.MudrodEngine;

public class SparkSVM {

  public SparkSVM() {
    // TODO Auto-generated constructor stub
  }

  public static void main(String[] args) {
    System.setProperty("hadoop.home.dir","C:\\winutils");
    MudrodEngine me = new MudrodEngine();
    //me.loadConfig();
    
    JavaSparkContext jsc = me.startSparkDriver().sc;
    //SparkContext sc = JavaSparkContext.toSparkContext(jsc);
    
    String path = "C:/mudrodCoreTestData/rankingResults/inputDataForSVM_spark.txt";
    JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(jsc.sc(), path).toJavaRDD();

    // Split initial RDD into two... [60% training data, 40% testing data].
    /*JavaRDD<LabeledPoint> training = data.sample(false, 0.6, 11L);
    training.cache();
    JavaRDD<LabeledPoint> test = data.subtract(training);*/

    // Run training algorithm to build the model.
    int numIterations = 100;
    //final SVMModel model = SVMWithSGD.train(training.rdd(), numIterations);
    final SVMModel model = SVMWithSGD.train(data.rdd(), numIterations);

    // Save and load model
    model.save(jsc.sc(), "C:/mudrodCoreTestData/rankingResults/model/javaSVMWithSGDModel");
    
    //SVMModel Model = SVMModel.load(jsc.sc(), "C:/mudrodCoreTestData/rankingResults/model/javaSVMWithSGDModel");
    
    jsc.sc().stop();

  }

}
