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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import esiptestbed.mudrod.discoveryengine.MudrodAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.integration.LinkageIntegration;
import esiptestbed.mudrod.main.MudrodEngine;

/**
 * Supports ability to transform regular user query into a semantic query
 */
public class Dispatcher extends MudrodAbstract {
  private static final Logger LOG = LoggerFactory.getLogger(Dispatcher.class);
  
  /**
   * Constructor supporting a number of parameters documented below.
   * @param config a {@link java.util.Map} containing K,V of type String, String respectively.
   * @param es the {@link esiptestbed.mudrod.driver.ESDriver} used to persist log files.
   * @param spark the {@link esiptestbed.mudrod.driver.SparkDriver} used to process input log files.
   */
  public Dispatcher(Map<String, String> config, ESDriver es, SparkDriver spark) {
    super(config, es, spark);
  }

  /**
   * Method of getting semantically most related terms by number
   * @param input regular input query
   * @param num the number of most related terms
   * @return
   */
  public Map<String, Double> getRelatedTerms(String input, int num) {
    LinkageIntegration li = new LinkageIntegration(this.config,
        this.es, null);
    Map<String, Double> sortedMap = li.appyMajorRule(input);
    Map<String, Double> selected_Map = new HashMap<>();
    int count = 0;
    for (Entry<String, Double> entry : sortedMap.entrySet()) {
      if (count < num) {
        selected_Map.put(entry.getKey(), entry.getValue());
      }
      count++;
    }
    return selected_Map;
  }

  /**
   * Method of getting semantically most related terms by similarity threshold
   * @param input regular input query
   * @param T value of threshold, raning from 0 to 1
   * @return
   */
  public Map<String, Double> getRelatedTermsByT(String input, double T) {
    LinkageIntegration li = new LinkageIntegration(this.config,
        this.es, null);
    Map<String, Double> sortedMap = li.appyMajorRule(input);
    Map<String, Double> selected_Map = new HashMap<>();

    for (Entry<String, Double> entry : sortedMap.entrySet()) {
      if (entry.getValue() >= T) {
        selected_Map.put(entry.getKey(), entry.getValue());
      }
    }
    return selected_Map;
  }

  /**
   * Method of creating semantic query based on Threshold
   * @param input regular query
   * @param num the number of most related terms
   * @return an semantic Elasticsearch query clause
   */
  public BoolQueryBuilder createSemQuery(String input, int num){
    Map<String, Double> selected_Map = getRelatedTermsByT(input, 0.8);
    selected_Map.put(input, (double) 1);

    String fieldsList[] = {"Dataset-Metadata", "Dataset-ShortName", "Dataset-LongName", "Dataset-Description", "DatasetParameter-*"};
    BoolQueryBuilder qb = new BoolQueryBuilder();
    for (Entry<String, Double> entry : selected_Map.entrySet()){
      qb.should(QueryBuilders.multiMatchQuery(entry.getKey(), fieldsList)
          .boost(entry.getValue().floatValue())
          .type(MultiMatchQueryBuilder.Type.PHRASE)
          .tieBreaker((float) 0.5)); // when set to 1.0, it would be equal to "most fields" query
    }
    
    LOG.info(qb.toString());
    return qb;
  }

  /**
   * Method of creating semantic query based on Threshold, and dataset shortname
   * @param input regular query
   * @param num the number of most related terms
   * @param shortName dataset shortName
   * @return
   */
  public QueryBuilder createQueryForClicks(String input, int num, String shortName){   
    Map<String, Double> selected_Map = getRelatedTermsByT(input, 0.8);
    selected_Map.put(input, (double) 1);

    BoolFilterBuilder bf = new BoolFilterBuilder();
    bf.must(FilterBuilders.termFilter("dataID", shortName));

    for (Map.Entry<String, Double> entry : selected_Map.entrySet()){
      bf.should(FilterBuilders.termFilter("query", entry.getKey()));      
    }
    QueryBuilder click_search = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), bf);

    return click_search;
  }


}
