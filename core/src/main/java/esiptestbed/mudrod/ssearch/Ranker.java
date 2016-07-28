package esiptestbed.mudrod.ssearch;

import java.util.Map;
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
  
  public void setResultList(List<SResult> rl)
  {
    resultList = rl;
  }

  double getMean(String attribute)
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
    return sum/resultList.size();
  }

  double getVariance(String attribute)
  {
    double mean = getMean(attribute);
    Double temp = 0.0;
    for(SResult a :resultList)
      temp += (mean-(Double)SResult.get(a, attribute))*(mean-(Double)SResult.get(a, attribute));
    return temp/resultList.size();
  }

  double getStdDev(String attribute)
  {
    return Math.sqrt(getVariance(attribute));
  }

  public List<SResult> rank()
  {
    double relevance_mean = getMean("relevance");
    double clicks_mean = getMean("clicks");
    //double release_mean = getMean("dateLong");

    double relevance_std = getStdDev("relevance");
    double clicks_std = getStdDev("clicks");
    //double release_std = getStdDev("dateLong");


    for(int i=0; i< resultList.size(); i++)
    {
      if(relevance_std!=0)
      {
        resultList.get(i).term_score = (resultList.get(i).relevance - relevance_mean)/relevance_std;
      }
      else
      {
        resultList.get(i).term_score = (double) 0;
      }

      if(clicks_std!=0)
      {
        resultList.get(i).click_score = (resultList.get(i).clicks - clicks_mean)/clicks_std;
      }
      else
      {
        resultList.get(i).click_score = (double) 0;
      }

      /*if(release_std!=0)
      {
        resultList.get(i).releaseDate_score = (resultList.get(i).dateLong - release_mean)/release_std;
      }
      else
      {
        resultList.get(i).releaseDate_score = (double) 0;
      }*/

      resultList.get(i).final_score = resultList.get(i).term_score + resultList.get(i).click_score + resultList.get(i).releaseDate_score;
    }
    
    Collections.sort(resultList, new ResultComparator());
    
    return resultList;
  }

}
