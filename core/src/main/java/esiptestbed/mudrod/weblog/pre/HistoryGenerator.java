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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Supports ability to generate search history (queries) for each individual user (IP)
 */
public class HistoryGenerator extends DiscoveryStepAbstract {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(HistoryGenerator.class);

  /**
   * Constructor supporting a number of parameters documented below.
   * @param config a {@link java.util.Map} containing K,V of type String, String respectively.
   * @param es the {@link esiptestbed.mudrod.driver.ESDriver} used to persist log files.
   * @param spark the {@link esiptestbed.mudrod.driver.SparkDriver} used to process input log files.
   */
  public HistoryGenerator(Map<String, String> config, ESDriver es,
      SparkDriver spark) {
    super(config, es, spark);
  }

  @Override
  public Object execute() {
    LOG.info("*****************HistoryGenerator starts******************");
    startTime = System.currentTimeMillis();

    GenerateBinaryMatrix();

    endTime = System.currentTimeMillis();
    LOG.info("*****************HistoryGenerator ends******************Took {}s", (endTime - startTime) / 1000);
    return null;
  }

  /**
   * Method to generate a binary user*query matrix (stored in temporary .csv file)
   */
  public void GenerateBinaryMatrix() {
    try {
      File file = new File(config.get("userHistoryMatrix"));
      if (file.exists()) {
        file.delete();
      }

      file.createNewFile();

      FileWriter fw = new FileWriter(file.getAbsoluteFile());
      BufferedWriter bw = new BufferedWriter(fw);

      ArrayList<String> cleanupTypeList = es.getTypeListWithPrefix(
          config.get("indexName"), config.get("SessionStats_prefix"));

      bw.write("Num" + ",");

      // step 1: write first row of csv
      SearchResponse sr = es.client.prepareSearch(config.get("indexName"))
    	  .setTypes(cleanupTypeList.toArray(new String[0]))
          .setQuery(QueryBuilders.matchAllQuery()).setSize(0)
          .addAggregation(AggregationBuilders.terms("IPs").field("IP").size(0))
          .execute().actionGet();
      Terms IPs = sr.getAggregations().get("IPs");
      List<String> ipList = new ArrayList<>();
      for (Terms.Bucket entry : IPs.getBuckets()) {
        if (entry.getDocCount() > Integer
            .parseInt(config.get("mini_userHistory"))) { // filter out less
          // active users/ips
          ipList.add(entry.getKey());
        }
      }

      bw.write(String.join(",", ipList) + "\n");

      // step 2: step the rest rows of csv
      SearchResponse sr_2 = es.client.prepareSearch(config.get("indexName"))
    	  .setTypes(cleanupTypeList.toArray(new String[0]))
    	  .setQuery(QueryBuilders.matchAllQuery()).setSize(0)
          .addAggregation(AggregationBuilders.terms("KeywordAgg")
              .field("keywords").size(0).subAggregation(
                  AggregationBuilders.terms("IPAgg").field("IP").size(0)))
          .execute().actionGet();
      Terms keywords = sr_2.getAggregations().get("KeywordAgg");
      for (Terms.Bucket keyword : keywords.getBuckets()) {
        Map<String, Integer> IP_map = new HashMap<String, Integer>();
        Terms IPAgg = keyword.getAggregations().get("IPAgg");

        int distinct_user = IPAgg.getBuckets().size();
        if (distinct_user > Integer.parseInt(config.get("mini_userHistory")))
        {
          bw.write(keyword.getKey() + ",");
          for (Terms.Bucket IP : IPAgg.getBuckets()) {

            IP_map.put(IP.getKey(), 1);
          }
          for (int i = 0; i < ipList.size(); i++) {
            if (IP_map.containsKey(ipList.get(i))) {
              bw.write(IP_map.get(ipList.get(i)) + ",");
            } else {
              bw.write("0,");
            }
          }
          bw.write("\n");
        }
      }

      bw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public Object execute(Object o) {
    return null;
  }

}
