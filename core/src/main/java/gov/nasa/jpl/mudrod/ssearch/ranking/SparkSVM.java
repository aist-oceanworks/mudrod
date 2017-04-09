/*
 * Licensed under the Apache License, Version 2.0 (the "License"); you 
 * may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gov.nasa.jpl.mudrod.ssearch.ranking;

import gov.nasa.jpl.mudrod.main.MudrodEngine;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;

public class SparkSVM {

  private SparkSVM() {
    //public constructor
  }

  public static void main(String[] args) {
    MudrodEngine me = new MudrodEngine();

    JavaSparkContext jsc = me.startSparkDriver().sc;

    String path = SparkSVM.class.getClassLoader().getResource("inputDataForSVM_spark.txt").toString();
    JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(jsc.sc(), path).toJavaRDD();

    // Run training algorithm to build the model.
    int numIterations = 100;
    final SVMModel model = SVMWithSGD.train(data.rdd(), numIterations);

    // Save and load model
    model.save(jsc.sc(), SparkSVM.class.getClassLoader().getResource("javaSVMWithSGDModel").toString());

    jsc.sc().stop();

  }

}
