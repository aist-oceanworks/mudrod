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
package esiptestbed.mudrod.weblog.pre;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;

public class HistoryGenerator extends DiscoveryStepAbstract {

  public HistoryGenerator(Map<String, String> config, ESDriver es,
      SparkDriver spark) {
    super(config, es, spark);
    // TODO Auto-generated constructor stub
  }

  @Override
  public Object execute() {
    // TODO Auto-generated method stub
    System.out
        .println("*****************HistoryGenerator starts******************");
    startTime = System.currentTimeMillis();

    GenerateBinaryMatrix();

    endTime = System.currentTimeMillis();
    System.out
        .println("*****************HistoryGenerator ends******************Took "
            + (endTime - startTime) / 1000 + "s");
    return null;
  }

  public void GenerateBinaryMatrix() {
    try {
      File file = new File(config.get("userHistoryMatrix"));
      if (file.exists()) {
        file.delete();
      }

      file.createNewFile();

      FileWriter fw = new FileWriter(file.getAbsoluteFile());
      BufferedWriter bw = new BufferedWriter(fw);

      ArrayList<String> cleanup_typeList = es.getTypeListWithPrefix(
          config.get("indexName"), config.get("SessionStats"));

      bw.write("Num" + ",");

      // step 1: write first row of csv
      SearchResponse sr = es.client.prepareSearch(config.get("indexName"))
          .setTypes(String.join(", ", cleanup_typeList))
          .setQuery(QueryBuilders.matchAllQuery()).setSize(0)
          .addAggregation(AggregationBuilders.terms("IPs").field("IP").size(0)) // important
                                                                                // to
                                                                                // set
                                                                                // size
                                                                                // 0,
                                                                                // sum_other_doc_count
          .execute().actionGet();
      Terms IPs = sr.getAggregations().get("IPs");
      List<String> IPList = new ArrayList<String>();
      for (Terms.Bucket entry : IPs.getBuckets()) {
        if (entry.getDocCount() > Integer
            .parseInt(config.get("mini_userHistory"))) { // filter out less
                                                         // active users/ips
          IPList.add(entry.getKey());
        }
      }

      bw.write(String.join(",", IPList) + "\n");

      /*
       * bw.write("Num,"); for(int k=0; k< IPList.size(); k++){
       * if(k!=IPList.size()-1){ bw.write("f" + k + ","); }else{ bw.write("f" +
       * k + "\n"); }
       * 
       * }
       */

      // step 2: step the rest rows of csv
      SearchResponse sr_2 = es.client.prepareSearch(config.get("indexName"))
          .setTypes(String.join(", ", cleanup_typeList))
          .setQuery(QueryBuilders.matchAllQuery()).setSize(0)
          .addAggregation(AggregationBuilders.terms("KeywordAgg")
              .field("keywords").size(0).subAggregation(
                  AggregationBuilders.terms("IPAgg").field("IP").size(0))) // important
                                                                           // to
                                                                           // set
                                                                           // size
                                                                           // 0,
                                                                           // sum_other_doc_count
          .execute().actionGet();
      Terms keywords = sr_2.getAggregations().get("KeywordAgg");
      for (Terms.Bucket keyword : keywords.getBuckets()) {
        Map<String, Integer> IP_map = new HashMap<String, Integer>();
        Terms IPAgg = keyword.getAggregations().get("IPAgg");

        int distinct_user = IPAgg.getBuckets().size();
        if (distinct_user > Integer.parseInt(config.get("mini_userHistory"))) // filter
                                                                              // out
                                                                              // less
                                                                              // active
                                                                              // queries
        {
          bw.write(keyword.getKey() + ",");
          for (Terms.Bucket IP : IPAgg.getBuckets()) {

            IP_map.put(IP.getKey(), 1);
          }
          for (int i = 0; i < IPList.size(); i++) {
            if (IP_map.containsKey(IPList.get(i))) {
              bw.write(IP_map.get(IPList.get(i)) + ",");
            } else {
              bw.write("0,");
            }
          }
          bw.write("\n");
        }
      }

      bw.close();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public Object execute(Object o) {
    // TODO Auto-generated method stub
    return null;
  }

}
