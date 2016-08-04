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
import esiptestbed.mudrod.ssearch.structure.SResult;

public class Ranker extends MudrodAbstract {
  List<SResult> resultList = new ArrayList<SResult>();

  public Ranker(Map<String, String> config, ESDriver es, SparkDriver spark) {
    super(config, es, spark);
    // TODO Auto-generated constructor stub
  }

  public class ResultComparator implements Comparator<SResult> {
    @Override
    public int compare(SResult o1, SResult o2) {
      return Double.compare(o2.final_score, o1.final_score);
    }
  }

  private double getMean(String attribute, List<SResult> resultList)
  {
    double sum = 0.0;
    for(SResult a : resultList)
    {
      if(attribute.equals("dateLong"))
      {
        sum += ((Long)SResult.get(a, attribute)).doubleValue();
      }else{
        sum += (double)SResult.get(a, attribute);
      }      
    }
    return getNDForm(sum/resultList.size());
  }

  private double getVariance(String attribute, List<SResult> resultList)
  {
    double mean = getMean(attribute, resultList);
    double temp = 0.0;
    double val = 0.0;
    for(SResult a :resultList)
    {
      if(attribute.equals("dateLong"))
      {
        val = ((Long)SResult.get(a, attribute)).doubleValue();
      }else{
        val = (Double)SResult.get(a, attribute);
      }
      temp += (mean - val)*(mean - val);
    }

    return getNDForm(temp/resultList.size());
  }

  private double getStdDev(String attribute, List<SResult> resultList)
  {
    return getNDForm(Math.sqrt(getVariance(attribute, resultList)));
  }

  private double getNDForm(double d)
  {
    DecimalFormat NDForm = new DecimalFormat("#.###");
    return Double.valueOf(NDForm.format(d));
  }

  public List<SResult> rank(List<SResult> resultList)
  {
    double relevance_mean = getMean("relevance", resultList);
    double clicks_mean = getMean("clicks", resultList);
    double release_mean = getMean("dateLong", resultList);

    double relevance_std = getStdDev("relevance", resultList);
    double clicks_std = getStdDev("clicks", resultList);
    double release_std = getStdDev("dateLong", resultList);

    for(int i=0; i< resultList.size(); i++)
    {
      if(relevance_std!=0)
      {
        resultList.get(i).term_score = getNDForm((resultList.get(i).relevance - relevance_mean)/relevance_std);
      }

      if(clicks_std!=0)
      {
        resultList.get(i).click_score = getNDForm((resultList.get(i).clicks - clicks_mean)/clicks_std);
      }

      if(release_std!=0)
      {
        resultList.get(i).releaseDate_score = getNDForm((resultList.get(i).dateLong - release_mean)/release_std);
      }

      resultList.get(i).final_score = getNDForm(resultList.get(i).term_score + 
          0.5*resultList.get(i).click_score + 
          resultList.get(i).releaseDate_score);
    }

    Collections.sort(resultList, new ResultComparator());
    return resultList;
  }

}
