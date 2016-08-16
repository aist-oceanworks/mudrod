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
package esiptestbed.mudrod.ssearch;

import java.util.Map;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import esiptestbed.mudrod.discoveryengine.MudrodAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.ssearch.ranking.Learner;
import esiptestbed.mudrod.ssearch.structure.SResult;

/**
 * Supports the ability to calculating ranking score
 */
public class Ranker extends MudrodAbstract {
  List<SResult> resultList = new ArrayList<SResult>();
  String learnerType = null;
  Learner le = null;
  
  /**
* Constructor supporting a number of parameters documented below.
   * @param config a {@link java.util.Map} containing K,V of type String, String respectively.
   * @param es the {@link esiptestbed.mudrod.driver.ESDriver} used to persist log files.
   * @param spark the {@link esiptestbed.mudrod.driver.SparkDriver} used to process input log files.
   * @param learnerType the type of ML classifier
   */
  public Ranker(Map<String, String> config, ESDriver es, SparkDriver spark, String learnerType) {
    super(config, es, spark);
    this.learnerType = learnerType;
    le = new Learner(learnerType);
  }

  /**
   * Method of comparing results based on final score
   */
  public class ResultComparator implements Comparator<SResult> {
    @Override
    public int compare(SResult o1, SResult o2) {
      if(learnerType.equals("pointwise"))
      {
      double[] ins1 = {o1.term_score, o1.click_score, 
          o1.releaseDate_score, o1.allPop_score, 
          o1.monthPop_score, o1.userPop_score, o1.prediction};
      o1.final_score = le.classify(ins1);
      
      double[] ins2 = {o2.term_score, o2.click_score, 
          o2.releaseDate_score, o2.allPop_score, 
          o2.monthPop_score, o2.userPop_score, o2.prediction};
      o2.final_score = le.classify(ins2);
      
      return Double.compare(o2.final_score, o1.final_score);
      }
      else if(learnerType.equals("pairwise"))
      {
        double[] ins = {o2.term_score - o1.term_score,
            
            o2.Dataset_LongName_score - o1.Dataset_LongName_score,
            o2.Dataset_Metadata_score - o1.Dataset_Metadata_score,
            o2.DatasetParameter_Term_score - o1.DatasetParameter_Term_score,
            o2.DatasetSource_Source_LongName_score - o1.DatasetSource_Source_LongName_score,
            o2.DatasetSource_Sensor_LongName_score - o1.DatasetSource_Sensor_LongName_score,
            
            o2.click_score - o1.click_score, 
            o2.releaseDate_score - o1.releaseDate_score, 
            
            o2.version_score - o1.version_score, 
            o2.processingL_score - o1.processingL_score, 
            
            o2.allPop_score - o1.allPop_score, 
            o2.monthPop_score - o1.monthPop_score, 
            o2.userPop_score - o1.userPop_score, 
            o2.prediction - o1.prediction};
        double prediction = le.classify(ins);
        if(prediction == 1.0)  //prediction would be 0, 1 corresponding to -1, 1
        {
          return 0;
        }else{
          return 1;
        }
      }
      return 0;
    }
  }
  
  /**
   * Method of calculating mean value
   * @param attribute the attribute name that need to be calculated on
   * @param resultList an array list of result
   * @return mean value
   */
  private double getMean(String attribute, List<SResult> resultList)
  {
    double sum = 0.0;
    for(SResult a : resultList)
    { 
        sum += (double)SResult.get(a, attribute);    
    }
    return getNDForm(sum/resultList.size());
  }

  /**
   * Method of calculating variance value
   * @param attribute the attribute name that need to be calculated on
   * @param resultList an array list of result
   * @return variance value
   */
  private double getVariance(String attribute, List<SResult> resultList)
  {
    double mean = getMean(attribute, resultList);
    double temp = 0.0;
    double val = 0.0;
    for(SResult a :resultList)
    {    
      val = (Double)SResult.get(a, attribute);
      temp += (mean - val)*(mean - val);
    }

    return getNDForm(temp/resultList.size());
  }
  
  /**
   * Method of calculating standard variance
   * @param attribute the attribute name that need to be calculated on
   * @param resultList an array list of result
   * @return standard variance
   */
  private double getStdDev(String attribute, List<SResult> resultList)
  {
    return getNDForm(Math.sqrt(getVariance(attribute, resultList)));
  }
  
  /**
   * Method of calculating Z score
   * @param val the value of an attribute
   * @param mean the mean value of an attribute
   * @param std the standard deviation of an attribute
   * @return Z score
   */
  private double getZscore(double val, double mean, double std)
  {
    if(std!=0)
    {
      return getNDForm((val-mean)/std);
    }
    else
    {
      return 0;
    }
  }

  /**
   * Get the first N decimals of a double value
   * @param d double value that needs to be processed
   * @return processed double value
   */
  private double getNDForm(double d)
  {
    DecimalFormat NDForm = new DecimalFormat("#.###");
    return Double.valueOf(NDForm.format(d));
  }

  /**
   * Method of ranking a list of result
   * @param resultList result list
   * @return ranked result list
   */
  public List<SResult> rank(List<SResult> resultList)
  {
    for(int i=0; i< resultList.size(); i++)
    {
      resultList.get(i).term_score = getZscore(resultList.get(i).relevance, 
          getMean("relevance", resultList), 
          getStdDev("relevance", resultList));
            
      resultList.get(i).Dataset_LongName_score = getZscore(resultList.get(i).Dataset_LongName, 
          getMean("Dataset_LongName", resultList), 
          getStdDev("Dataset_LongName", resultList));
      
      resultList.get(i).Dataset_Metadata_score = getZscore(resultList.get(i).Dataset_Metadata, 
          getMean("Dataset_Metadata", resultList), 
          getStdDev("Dataset_Metadata", resultList));
      
      resultList.get(i).DatasetParameter_Term_score = getZscore(resultList.get(i).DatasetParameter_Term, 
          getMean("DatasetParameter_Term", resultList), 
          getStdDev("DatasetParameter_Term", resultList));
      
      resultList.get(i).DatasetSource_Source_LongName_score = getZscore(resultList.get(i).DatasetSource_Source_LongName, 
          getMean("DatasetSource_Source_LongName", resultList), 
          getStdDev("DatasetSource_Source_LongName", resultList));
      
      resultList.get(i).DatasetSource_Sensor_LongName_score = getZscore(resultList.get(i).DatasetSource_Sensor_LongName, 
          getMean("DatasetSource_Sensor_LongName", resultList), 
          getStdDev("DatasetSource_Sensor_LongName", resultList));
           
      resultList.get(i).click_score = getZscore(resultList.get(i).clicks, 
          getMean("clicks", resultList), 
          getStdDev("clicks", resultList));
      
      resultList.get(i).releaseDate_score = getZscore(resultList.get(i).dateLong, 
          getMean("dateLong", resultList), 
          getStdDev("dateLong", resultList));
      
      resultList.get(i).version_score = getZscore(resultList.get(i).versionNum, 
          getMean("versionNum", resultList), 
          getStdDev("versionNum", resultList));

      resultList.get(i).processingL_score = getZscore(resultList.get(i).proNum, 
          getMean("proNum", resultList), 
          getStdDev("proNum", resultList));
      
      resultList.get(i).monthPop_score = getZscore(resultList.get(i).monthPop, 
          getMean("monthPop", resultList), 
          getStdDev("monthPop", resultList));
      
      resultList.get(i).allPop_score = getZscore(resultList.get(i).allPop, 
          getMean("allPop", resultList), 
          getStdDev("allPop", resultList));
      
      resultList.get(i).userPop_score = getZscore(resultList.get(i).userPop, 
          getMean("userPop", resultList), 
          getStdDev("userPop", resultList));
    }

    Collections.sort(resultList, new ResultComparator());
    return resultList;
  }

}
