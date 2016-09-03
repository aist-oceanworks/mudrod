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

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import esiptestbed.mudrod.discoveryengine.MudrodAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.main.MudrodEngine;

/**
 * Supports the ability to importing training set into Elasticsearch
 */
public class TrainingImporter extends MudrodAbstract {

  /**
   * Constructor supporting a number of parameters documented below.
   * @param config a {@link java.util.Map} containing K,V of type String, String respectively.
   * @param es the {@link esiptestbed.mudrod.driver.ESDriver} used to persist log files.
   * @param spark the {@link esiptestbed.mudrod.driver.SparkDriver} used to process input log files.
   */
  public TrainingImporter(Properties props, ESDriver es,
      SparkDriver spark) {
    super(props, es, spark);
    es.deleteAllByQuery(props.getProperty("indexName"),
        "trainingranking", QueryBuilders.matchAllQuery());
    addMapping();
  }
  
  /**
   * Method of adding mapping to traning set type
   */
  public void addMapping() {
    XContentBuilder Mapping;
    try {
      Mapping = jsonBuilder().startObject()
          .startObject("trainingranking")
          .startObject("properties")
          .startObject("query")
          .field("type", "string").field("index", "not_analyzed")
          .endObject()
          .startObject("dataID")
          .field("type", "string").field("index", "not_analyzed")
          .endObject()
          .startObject("label").field("type", "string")
          .field("index", "not_analyzed")
          .endObject()
          .endObject().endObject().endObject();

      es.getClient().admin().indices().preparePutMapping(props.getProperty("indexName"))
          .setType("trainingranking").setSource(Mapping)
          .execute().actionGet();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  /**
   * Method of importing training set in to Elasticsearch
   * @param dataFolder the path to the traing set
   * @throws IOException
   */
  public void importTrainingSet(String dataFolder) throws IOException
  {
    es.createBulkProcesser();
    
    File[] files = new File(dataFolder).listFiles();
    for (File file : files) {
      BufferedReader br = new BufferedReader(new FileReader(file.getAbsolutePath()));
      br.readLine();
      String line = br.readLine();    
      while (line != null) {  
        String[] list = line.split(",");
        String query = file.getName().replace(".csv", "");
        if(list.length>0)
        {
        IndexRequest ir = new IndexRequest(props.getProperty("indexName"),
            "trainingranking")
                .source(
                    jsonBuilder().startObject()
                        .field("query", query)
                        .field("dataID", list[0])
                        .field("label", list[list.length-1])
                        .endObject());
        es.getBulkProcessor().add(ir);
        }
        line = br.readLine();
      }
      br.close();
    }
    es.destroyBulkProcessor();
  }
}
