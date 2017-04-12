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
package gov.nasa.jpl.mudrod.ssearch.ranking;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Supports ability to evaluating ranking results
 */
public class Evaluator {
  /**
   * Method of calculating NDCG score
   *
   * @param list a list of integer with each integer element indicating
   *             the performance at its position
   * @param K    the number of elements needed to be included in the calculation
   * @return NDCG score
   */
  public double getNDCG(int[] list, int K) {
    double dcg = this.getDCG(list, K);
    double idcg = this.getIDCG(list, K);
    double ndcg = 0.0;
    if (idcg > 0.0) {
      ndcg = dcg / idcg;
    }
    return ndcg;
  }

  /**
   * Method of getting the precision of a list at position K
   *
   * @param list a list of integer with each integer element indicating
   *             the performance at its position
   * @param K    the number of elements needed to be included in the calculation
   * @return precision at K
   */
  public double getPrecision(int[] list, int K) {
    int size = list.length;
    if (size == 0 || K == 0) {
      return 0;
    }

    if (K > size) {
      K = size;
    }

    int rel_doc_num = this.getRelevantDocNum(list, K);
    double precision = (double) rel_doc_num / (double) K;
    return precision;
  }

  /**
   * Method of getting the number of relevant element in a ranking results
   *
   * @param list a list of integer with each integer element indicating
   *             the performance at its position
   * @param K    the number of elements needed to be included in the calculation
   * @return the number of relevant element
   */
  private int getRelevantDocNum(int[] list, int K) {
    int size = list.length;
    if (size == 0 || K == 0) {
      return 0;
    }

    if (K > size) {
      K = size;
    }

    int rel_num = 0;
    for (int i = 0; i < K; i++) {
      if (list[i] > 3) { // 3 refers to "OK"
        rel_num++;
      }
    }
    return rel_num;
  }

  /**
   * Method of calculating DCG score from a list of ranking results
   *
   * @param list a list of integer with each integer element indicating
   *             the performance at its position
   * @param K    the number of elements needed to be included in the calculation
   * @return DCG score
   */
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
      int pos = i + 1;
      double rel_log = Math.log(pos) / Math.log(2);
      dcg += rel / rel_log;
    }
    return dcg;
  }

  /**
   * Method of calculating ideal DCG score from a list of ranking results
   *
   * @param list a list of integer with each integer element indicating
   *             the performance at its position
   * @param K    the number of elements needed to be included in the calculation
   * @return IDCG score
   */
  private double getIDCG(int[] list, int K) {
    Comparator<Integer> comparator = new Comparator<Integer>() {
      @Override
      public int compare(Integer o1, Integer o2) {
        return o2.compareTo(o1);
      }
    };
    List<Integer> sortlist = IntStream.of(list).boxed().collect(Collectors.toList());
    ;
    Collections.sort(sortlist, comparator);
    int[] sortedArr = sortlist.stream().mapToInt(i -> i).toArray();
    double idcg = this.getDCG(sortedArr, K);
    return idcg;
  }

}
