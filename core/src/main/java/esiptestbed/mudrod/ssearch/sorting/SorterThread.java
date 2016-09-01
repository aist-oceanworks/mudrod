package esiptestbed.mudrod.ssearch.sorting;

import java.util.ArrayList;
import java.util.List;

import esiptestbed.mudrod.ssearch.ranking.Learner;
import esiptestbed.mudrod.ssearch.structure.SResult;

public class SorterThread extends Thread{
  private int start_index = 0;
  private int end_index = 0;
  private List<SResult> List = null;
  private Learner le = null;

  public SorterThread(List<SResult> list, int start, int end, Learner learner) {
    List = list;
    start_index = start;
    end_index = end;
    le = learner;
  }
  
  public void run(){  
    for(int j = start_index; j <= end_index; j++)
    {
      for(int k = 0; k< List.size(); k++)
      {
        if(k!=j)
        {
          List.get(j).below += comp (List.get(j), List.get(k));
        }
      }
    }
  } 
  
  /**
   * Method of compare two search resutls
   * @param o1 search result 1
   * @param o2 search result 2
   * @return 1 if o1>o2, 0 otherwise
   */
  public int comp(SResult o1, SResult o2)
  {
    List<Double> instList = new ArrayList<Double>();
    for(int i =0; i <SResult.rlist.length; i++)
    {
      double o2_score = SResult.get(o2, SResult.rlist[i]);
      double o1_score = SResult.get(o1, SResult.rlist[i]);
      instList.add(o2_score - o1_score);
    }
    instList.add(o2.prediction);
    double[] ins = instList.stream().mapToDouble(i->i).toArray();
    
    double prediction = le.classify(ins);
    if(prediction == 1.0)  
    {
      return 1;
    }else{
      return 0;
    }
  }

}
