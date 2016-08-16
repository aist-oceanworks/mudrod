package esiptestbed.mudrod.ssearch.ranking;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Evaluator {
  public Evaluator() {
    // TODO Auto-generated constructor stub
  }

  public double getNDCG(int[] list, int K) {
    double dcg = this.getDCG(list, K);
    double idcg = this.getIDCG(list, K);
    double ndcg = 0.0;
    if (idcg > 0.0) {
      ndcg = dcg / idcg;
    }

    return ndcg;
  }

  public double getAvePrecision(int[] list, int K) {
    double precisions = 0.0;
    for (int i = 0; i < K; i++) {
      precisions += list[i] * this.getPosPrecision(list, i+1);
    }

    int rel_doc_num = this.getRelevantDocNum(list, K);
    double ave_precision = precisions/rel_doc_num;
    return ave_precision;
  }

  private double getPosPrecision(int[] list, int K){
    int rel_doc_num = this.getRelevantDocNum(list, K);
    double precision = (double)rel_doc_num/(double)K;
    return precision;
  }

  private int getRelevantDocNum(int[] list, int K){
    int size = list.length;
    if (size == 0 || K == 0) {
      return 0;
    }

    if (K > size) {
      K = size;
    }

    int rel_num = 0;
    for (int i = 0; i < K; i++) {
      if(list[i]> 0){
        rel_num += 1;
      }
    }

    return rel_num; 
  }

  private double getDCG(int[] list, int K) {
    int size = list.length;
    if (size == 0 || K == 0) {
      return 0.0;
    }

    if (K > size) {
      K = size;
    }

    double dcg = list[0];
    for (int i = 1; i < K; i++) {
      int rel = list[i];
      int pos = i+1;
      double rel_log = Math.log(pos)/Math.log(2);
      dcg += rel / rel_log;
    }
    return dcg;
  }

  private double getIDCG(int[] list, int K) {
    Comparator<Integer> comparator = new Comparator<Integer>() {
      @Override
      public int compare(Integer o1, Integer o2) {
        return o2.compareTo(o1);
      }
    };

    List<Integer> sortlist = IntStream.of(list).boxed().collect(Collectors.toList());;
    Collections.sort(sortlist, comparator);
    int[] sortedArr = sortlist.stream().mapToInt(i->i).toArray();
    double idcg = this.getDCG(sortedArr, K);
    return idcg;
  }

  public static void main(String[] args) {
    // TODO Auto-generated method stub
    Evaluator eva = new Evaluator();
    int[] list = {3, 2, 3, 0, 1 ,2};
    System.out.println(eva.getNDCG(list, 6));
  }

}
