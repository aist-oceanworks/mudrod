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

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;

import esiptestbed.mudrod.main.MudrodEngine;
import esiptestbed.mudrod.ssearch.structure.SResult;
import libsvm.svm_model;
import libsvm.svm_node;
import weka.classifiers.Classifier;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;

/**
 * Supports the ability to importing classifier into memory
 */
public class Learner {
  private Classifier cls= null;
  private static final String POINTWISE = "pointwise";
  private static final String PAIRWISE = "pairwise";

  /**
   * Constructor to load in weka classifier
   * @param classifierName classifier type
   */
  public Learner(String classifierName) {
    String rootPath="C:/mudrodCoreTestData/rankingResults/model/"; 
    
    try {
      if(classifierName.equals(POINTWISE))
      {
        cls = (Classifier) weka.core.SerializationHelper.read(rootPath+"linearRegression.model");
      }
      else if(classifierName.equals(PAIRWISE))
      {
        /*URL clsURL = Learner.class.getClassLoader().getResource("rankSVMmodel_new.model");
        File file = new File(clsURL.toURI());
        String clspath = file.getAbsolutePath();       
        cls = (Classifier) weka.core.SerializationHelper.read(clspath);*/
        cls = (Classifier) weka.core.SerializationHelper.read(rootPath+"rankSVM_7att_no_termAndv.model");       
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Method of creating dataset which is necessary for further classification
   * @return an empty instances
   */
  public Instances createDataset(){
    ArrayList<Attribute> attributes = new ArrayList<Attribute>();
    for(int i =0; i <SResult.rlist.length; i++)
    {
      attributes.add(new Attribute(SResult.rlist[i]));
    }  
    attributes.add(new Attribute("Label"));
    Instances dataset = new Instances("train_dataset", attributes, 0);
    dataset.setClassIndex(dataset.numAttributes() - 1);
    return dataset;
  }

  /**
   * Method of classifying instance
   * @param instance the instance that needs to be classified
   * @return the class id
   */
  public double classify(double[] instance)
  {
    Instances dataset = createDataset();
    double prediction = 0;
    try {
      Instance inst = new DenseInstance(1.0, instance);
      dataset.add(inst);
      prediction = cls.classifyInstance(dataset.instance(0));
    } catch (Exception e) {
      e.printStackTrace();
    }
    return prediction;
  }

}
