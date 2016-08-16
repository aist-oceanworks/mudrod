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

import java.util.ArrayList;

import weka.classifiers.Classifier;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;

public class Learner {
  private Classifier cls= null;
  private static final String POINTWISE = "pointwise";
  private static final String PAIRWISE = "pairwise";
  private static final String ORDINALCLASSIFIER = "ordinal";

  public Learner(String classifierName) {
    String rootPath="C:/mudrodCoreTestData/rankingResults/NewEvaluation/"; 
    
    try {
      if(classifierName.equals(POINTWISE))
      {
        cls = (Classifier) weka.core.SerializationHelper.read(rootPath+"linearRegressionModel.model");
      }
      else if(classifierName.equals(PAIRWISE))
      {
        cls = (Classifier) weka.core.SerializationHelper.read(rootPath+"nonLinearSVM.model");
      }
      else if(classifierName.equals(ORDINALCLASSIFIER))
      {
        cls = (Classifier) weka.core.SerializationHelper.read(rootPath+"ordinalClassifierModel.model");
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public Instances createDataset(){
    ArrayList<Attribute> attributes = new ArrayList<Attribute>();

    attributes.add(new Attribute("term_score"));
    attributes.add(new Attribute("click_score"));
    attributes.add(new Attribute("releaseDate_score"));
    attributes.add(new Attribute("AllPop_score"));
    attributes.add(new Attribute("MonthPop_score"));
    attributes.add(new Attribute("UserPop_score"));    
    attributes.add(new Attribute("Label"));

    Instances dataset = new Instances("train_dataset", attributes, 0);
    dataset.setClassIndex(dataset.numAttributes() - 1);

    return dataset;
  }

  public double classify(double[] instance)
  {
    Instances dataset = createDataset();
    double prediction = 0;
    try {
      Instance inst = new DenseInstance(1.0, instance);
      dataset.add(inst);
      prediction = cls.classifyInstance(dataset.instance(0));
      //System.out.println("Predicted: " + prediction);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return prediction;
  }

}
