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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
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
   * @param list a list of integer with each integer element indicating 
   * the performance at its position
   * @param K the number of elements needed to be included in the calculation
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
   * @param list a list of integer with each integer element indicating 
   * the performance at its position
   * @param K the number of elements needed to be included in the calculation
   * @return precision at K
   */
  public double getPrecision(int[] list, int K){
    int size = list.length;
    if (size == 0 || K == 0) {
      return 0;
    }

    if (K > size) {
      K = size;
    }

    int rel_doc_num = this.getRelevantDocNum(list, K);
    double precision = (double)rel_doc_num/(double)K;
    return precision;
  }

  /**
   * Method of getting the number of relevant element in a ranking results
   * @param list a list of integer with each integer element indicating 
   * the performance at its position
   * @param K the number of elements needed to be included in the calculation
   * @return the number of relevant element
   */
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
      if(list[i] >= 6){ // 3 refers to "OK"
        rel_num++;
      }
    }
    return rel_num; 
  }

  /**
   * Method of calculating DCG score from a list of ranking results
   * @param list a list of integer with each integer element indicating 
   * the performance at its position
   * @param K the number of elements needed to be included in the calculation
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
      int pos = i+1;
      double rel_log = Math.log(pos)/Math.log(2);
      dcg += rel / rel_log;
    }
    return dcg;
  }

  /**
   * Method of calculating ideal DCG score from a list of ranking results
   * @param list a list of integer with each integer element indicating 
   * the performance at its position
   * @param K the number of elements needed to be included in the calculation
   * @return IDCG score
   */
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

  public int getRankNum(String str)
  {
    if(str.contains("Excellent"))
    {
      return 7;
    }else if(str.contains("Very good"))
    {
      return 6;
    }else if(str.contains("Good"))
    {
      return 5;
    }else if(str.contains("OK"))
    {
      return 4;
    }else if(str.contains("Bad"))
    {
      return 3;
    }else if(str.contains("Very bad"))
    {
      return 2;
    }else if(str.contains("Terrible"))
    {
      return 1;
    }

    return 0;
  }

  public static void main(String[] args) throws IOException{
    Evaluator eva = new Evaluator();    
    String learnerType = "SingleVar&click_score";
    String evaType = "precision";
    int eva_K = 200;

    String dataFolder = "C:/mudrodCoreTestData/rankingResults/Geotest/SingleVar&click_score";
    File file_eva = new File("C:/mudrodCoreTestData/rankingResults/Geotest/" + learnerType + "_" + evaType + ".csv");
    if (file_eva.exists()) {
      file_eva.delete();
      file_eva.createNewFile();
    }
    FileWriter fw_eva = new FileWriter(file_eva.getAbsoluteFile());
    BufferedWriter bw_eva = new BufferedWriter(fw_eva); 
    File[] files = new File(dataFolder).listFiles();
    for (File file : files) {
      BufferedReader br = new BufferedReader(new FileReader(file.getAbsolutePath()));
      String q = file.getName().replace(".csv", "");
      bw_eva.write(q + ",");
      List<Integer> arank_arrayList = new ArrayList<Integer>();
      br.readLine();
      String line = br.readLine();    
      while (line != null) {  
        String[] list = line.split(",");

        if(list.length>0)
        {
          arank_arrayList.add(eva.getRankNum(list[list.length-1]));
        }
        line = br.readLine();
      }

      int[] rank_array = arank_arrayList.stream().mapToInt(i->i).toArray(); 

      for(int j = 0; j < eva_K; j++)
      {
        if(evaType.equals("precision"))
        {
          if(j < (eva_K-1))
          {
            bw_eva.write(eva.getPrecision(rank_array, j + 1) + ",");
          }else{
            bw_eva.write(eva.getPrecision(rank_array, j + 1) + "\n");
          }
        }

        if(evaType.equals("ndcg"))
        {
          if(j < (eva_K-1))
          {
            bw_eva.write(eva.getNDCG(rank_array, j + 1) + ",");
          }else{
            bw_eva.write(eva.getNDCG(rank_array, j + 1) + "\n");
          }
        }
      }

      br.close();
    }
    bw_eva.close();

  }

}
