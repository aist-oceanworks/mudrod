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
package gov.nasa.jpl.mudrod.ssearch;

import gov.nasa.jpl.mudrod.discoveryengine.MudrodAbstract;
import gov.nasa.jpl.mudrod.driver.ESDriver;
import gov.nasa.jpl.mudrod.driver.SparkDriver;
import gov.nasa.jpl.mudrod.main.MudrodConstants;
import gov.nasa.jpl.mudrod.ssearch.ranking.Learner;
import gov.nasa.jpl.mudrod.ssearch.structure.SResult;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.*;

/**
 * Supports the ability to calculating ranking score
 */
public class Ranker extends MudrodAbstract implements Serializable {
  /**
   *
   */
  private static final long serialVersionUID = 1L;
  transient List<SResult> resultList = new ArrayList<>();

  String learnerType = null;
  Learner le = null;

  public Ranker(Properties props, ESDriver es, SparkDriver spark, String learnerType) {
    super(props, es, spark);
    this.learnerType = learnerType;
    le = new Learner(learnerType, spark, props.getProperty(MudrodConstants.SVM_SGD_MODEL));
  }

  /**
   * Method of comparing results based on final score
   */
  public class ResultComparator implements Comparator<SResult> {
    @Override
    public int compare(SResult o1, SResult o2) {
      return o2.below.compareTo(o1.below);
    }
  }

  /**
   * Method of calculating mean value
   *
   * @param attribute  the attribute name that need to be calculated on
   * @param resultList an array list of result
   * @return mean value
   */
  private double getMean(String attribute, List<SResult> resultList) {
    double sum = 0.0;
    for (SResult a : resultList) {
      sum += (double) SResult.get(a, attribute);
    }
    return getNDForm(sum / resultList.size());
  }

  /**
   * Method of calculating variance value
   *
   * @param attribute  the attribute name that need to be calculated on
   * @param resultList an array list of result
   * @return variance value
   */
  private double getVariance(String attribute, List<SResult> resultList) {
    double mean = getMean(attribute, resultList);
    double temp = 0.0;
    double val;
    for (SResult a : resultList) {
      val = (Double) SResult.get(a, attribute);
      temp += (mean - val) * (mean - val);
    }

    return getNDForm(temp / resultList.size());
  }

  /**
   * Method of calculating standard variance
   *
   * @param attribute  the attribute name that need to be calculated on
   * @param resultList an array list of result
   * @return standard variance
   */
  private double getStdDev(String attribute, List<SResult> resultList) {
    return getNDForm(Math.sqrt(getVariance(attribute, resultList)));
  }

  /**
   * Method of calculating Z score
   *
   * @param val  the value of an attribute
   * @param mean the mean value of an attribute
   * @param std  the standard deviation of an attribute
   * @return Z score
   */
  private double getZscore(double val, double mean, double std) {
    if (!equalComp(std, 0)) {
      return getNDForm((val - mean) / std);
    } else {
      return 0;
    }
  }

  private boolean equalComp(double a, double b) {
    return Math.abs(a - b) < 0.0001;
  }

  /**
   * Get the first N decimals of a double value
   *
   * @param d double value that needs to be processed
   * @return processed double value
   */
  private double getNDForm(double d) {
    DecimalFormat ndForm = new DecimalFormat("#.###");
    return Double.valueOf(ndForm.format(d));
  }

  /**
   * Method of ranking a list of result
   *
   * @param resultList result list
   * @return ranked result list
   */
  public List<SResult> rank(List<SResult> resultList) {
    for (int i = 0; i < resultList.size(); i++) {
      for (int m = 0; m < SResult.rlist.length; m++) {
        String att = SResult.rlist[m].split("_")[0];
        double val = SResult.get(resultList.get(i), att);
        double mean = getMean(att, resultList);
        double std = getStdDev(att, resultList);
        double score = getZscore(val, mean, std);
        String scoreId = SResult.rlist[m];
        SResult.set(resultList.get(i), scoreId, score);
      }
    }

    // using collection.sort directly would cause an "not transitive" error
    // this is because the training model is not a overfitting model
    for (int j = 0; j < resultList.size(); j++) {
      for (int k = 0; k < resultList.size(); k++) {
        if (k != j) {
          resultList.get(j).below += comp(resultList.get(j), resultList.get(k));
        }
      }
    }

    Collections.sort(resultList, new ResultComparator());
    return resultList;
  }

  /**
   * Method of compare two search resutls
   *
   * @param o1 search result 1
   * @param o2 search result 2
   * @return 1 if o1 is greater than o2, 0 otherwise
   */
  public int comp(SResult o1, SResult o2) {
    List<Double> instList = new ArrayList<>();
    for (int i = 0; i < SResult.rlist.length; i++) {
      double o2Score = SResult.get(o2, SResult.rlist[i]);
      double o1Score = SResult.get(o1, SResult.rlist[i]);
      instList.add(o2Score - o1Score);
    }

    double[] ins = instList.stream().mapToDouble(i -> i).toArray();
    LabeledPoint insPoint = new LabeledPoint(99.0, Vectors.dense(ins));
    double prediction = le.classify(insPoint);
    if (equalComp(prediction, 1)) { //different from weka where the return value is 1 or 2
      return 0;
    } else {
      return 1;
    }
  }

}
