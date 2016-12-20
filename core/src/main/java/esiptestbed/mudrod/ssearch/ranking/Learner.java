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
package esiptestbed.mudrod.ssearch.ranking;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.spark.SparkContext;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.regression.LabeledPoint;

import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.ssearch.structure.SResult;
import weka.classifiers.Classifier;
import weka.classifiers.meta.OrdinalClassClassifier;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;

/**
 * Supports the ability to importing classifier into memory
 */
public class Learner implements Serializable{
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private static final String SPARKSVM = "SparkSVM";
  SVMModel model = null;
  transient SparkContext sc = null;

  private static final String POINTWISE = "pointwise";
  private OrdinalClassClassifier cls= null;

  /**
   * Constructor to load in spark SVM classifier
   * @param classifierName classifier type
   * @param skd an instance of spark driver
   */
  public Learner(String classifierName, SparkDriver skd, String svmSgdModel) {
    if(classifierName.equals(SPARKSVM)) {
      sc = skd.sc.sc();
      sc.addFile(svmSgdModel, true);
      model = SVMModel.load(sc, svmSgdModel);
    }

    /*if(classifierName.equals(POINTWISE))
    {
      try {
        cls = (OrdinalClassClassifier) weka.core.SerializationHelper.read("C:/mudrodCoreTestData/rankingResults/model/OrdinalR_model1003.model");
      } catch (Exception e) {
        e.printStackTrace();
      }
    }*/
  }

  /*public Instances createDataset(){
    ArrayList<Attribute> attributes = new ArrayList<Attribute>();
    for(int i = 0; i <SResult.rlist.length; i++)
    {
      attributes.add(new Attribute(SResult.rlist[i]));
    }  
    attributes.add(new Attribute("label"));
    Instances dataset = new Instances("train_dataset", attributes, 0);
    dataset.setClassIndex(dataset.numAttributes() - 1);
    return dataset;
  }*/

  /**
   * Method of classifying instance
   * @param p the instance that needs to be classified
   * @return the class id
   */
  public double classify(LabeledPoint p) {
    return model.predict(p.features());
  }

  /*public double classify(double[] instance)
  {
    Instances dataset = createDataset();
    double prediction = 0;

    Instance inst = new DenseInstance(1.0, instance);
    dataset.add(inst);

    try {
      double re[] = cls.distributionForInstance(dataset.instance(0));
      System.out.println(re.length);
      for(int i = 0; i < re.length; i++)
      {
        System.out.println(re[i]);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return prediction;
  }*/

}
