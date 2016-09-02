package esiptestbed.mudrod.ssearch.sorting;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import esiptestbed.mudrod.ssearch.ranking.Learner;
import esiptestbed.mudrod.ssearch.structure.SResult;
import weka.classifiers.AbstractClassifier;

public class Sorter {
  private List<SResult> resultList = null;
  private int numofthread = 4;

  public Sorter(List<SResult> list) {
    resultList = list;
  }

  public class ResultComparator implements Comparator<SResult> {
    @Override
    public int compare(SResult o1, SResult o2) {
      return o2.below.compareTo(o1.below);
    }
  }

  public List<SResult> sort(Learner learner)
  {
    SorterThread[] threadlist = new SorterThread[numofthread];
    int num = resultList.size()/numofthread;
    int end_index = 0;
    for(int i=0; i<numofthread; i++)
    {
     /* List<SResult> newList = new ArrayList<>() ;
      for (int m = 0 ; m < resultList.size(); m++){
        SResult sr = new SResult(resultList.get(m));
        newList.add(sr);
      }*/

      if(i == numofthread -1)
      {
        end_index = resultList.size() - 1;
      }else{
        end_index = (i+1)*num-1;
      }
      
      try {
        learner.setClassifier(AbstractClassifier.makeCopy(learner.getClassifier()));
      } catch (Exception e) {
        e.printStackTrace();
      }
      threadlist[i] = new SorterThread(resultList, i*num, end_index, learner);    
      threadlist[i].start();
    }

    for(int j = 0; j < numofthread; j++) {
      try {
        threadlist[j].join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      } 
    }
    Collections.sort(resultList, new ResultComparator());
    return resultList;
  }
}
