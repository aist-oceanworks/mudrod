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
package gov.nasa.jpl.mudrod.weblog.pre;

import gov.nasa.jpl.mudrod.driver.ESDriver;
import gov.nasa.jpl.mudrod.driver.SparkDriver;
import gov.nasa.jpl.mudrod.main.MudrodConstants;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Supports ability to generate search history (queries) for each individual
 * user (IP)
 */
public class HistoryGenerator extends LogAbstract {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(HistoryGenerator.class);

  public HistoryGenerator(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  @Override
  public Object execute() {
    LOG.info("Starting HistoryGenerator...");
    startTime = System.currentTimeMillis();

    generateBinaryMatrix();

    endTime = System.currentTimeMillis();
    LOG.info("HistoryGenerator complete. Time elapsed {} seconds", (endTime - startTime) / 1000);
    return null;
  }

  /**
   * Method to generate a binary user*query matrix (stored in temporary .csv
   * file)
   */
  public void generateBinaryMatrix() {
    try {

      System.out.println(props.getProperty("userHistoryMatrix"));
      File file = new File(props.getProperty("userHistoryMatrix"));
      if (file.exists()) {
        file.delete();
      }

      file.createNewFile();

      FileWriter fw = new FileWriter(file.getAbsoluteFile());
      BufferedWriter bw = new BufferedWriter(fw);
      bw.write("Num" + ",");

      // step 1: write first row of csv
      List<String> logIndexList = es.getIndexListWithPrefix(props.getProperty(MudrodConstants.LOG_INDEX));

      String[] logIndices = logIndexList.toArray(new String[0]);
      String[] statictypeArray = new String[] { this.sessionStats };
      int docCount = es.getDocCount(logIndices, statictypeArray);

      SearchResponse sr = es.getClient().prepareSearch(logIndices).setTypes(statictypeArray).setQuery(QueryBuilders.matchAllQuery()).setSize(0)
          .addAggregation(AggregationBuilders.terms("IPs").field("IP").size(docCount)).execute().actionGet();
      Terms ips = sr.getAggregations().get("IPs");
      List<String> ipList = new ArrayList<>();
      for (Terms.Bucket entry : ips.getBuckets()) {
        if (entry.getDocCount() > Integer.parseInt(props.getProperty(MudrodConstants.MINI_USER_HISTORY))) { // filter
          // out
          // less
          // active users/ips
          ipList.add(entry.getKey().toString());
        }
      }
      bw.write(String.join(",", ipList) + "\n");

      // step 2: step the rest rows of csv
      SearchRequestBuilder sr2Builder = es.getClient().prepareSearch(logIndices).setTypes(statictypeArray).setQuery(QueryBuilders.matchAllQuery()).setSize(0)
          .addAggregation(AggregationBuilders.terms("KeywordAgg").field("keywords").size(docCount).subAggregation(AggregationBuilders.terms("IPAgg").field("IP").size(docCount)));

      SearchResponse sr2 = sr2Builder.execute().actionGet();
      Terms keywords = sr2.getAggregations().get("KeywordAgg");

      for (Terms.Bucket keyword : keywords.getBuckets()) {

        Map<String, Integer> ipMap = new HashMap<>();
        Terms ipAgg = keyword.getAggregations().get("IPAgg");

        int distinctUser = ipAgg.getBuckets().size();
        if (distinctUser > Integer.parseInt(props.getProperty(MudrodConstants.MINI_USER_HISTORY))) {
          bw.write(keyword.getKey() + ",");
          for (Terms.Bucket IP : ipAgg.getBuckets()) {

            ipMap.put(IP.getKey().toString(), 1);
          }
          for (int i = 0; i < ipList.size(); i++) {
            if (ipMap.containsKey(ipList.get(i))) {
              bw.write(ipMap.get(ipList.get(i)) + ",");
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
