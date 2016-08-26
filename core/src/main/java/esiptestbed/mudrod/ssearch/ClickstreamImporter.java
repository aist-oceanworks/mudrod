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

import java.util.Properties;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import esiptestbed.mudrod.discoveryengine.MudrodAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;

/**
 * Supports ability to import click stream data into Elasticsearch
 * through .csv file
 */
public class ClickstreamImporter extends MudrodAbstract {
  /**
   * Constructor supporting a number of parameters documented below.
   * @param config a {@link java.util.Map} containing K,V of type String, String respectively.
   * @param es the {@link esiptestbed.mudrod.driver.ESDriver} used to persist log files.
   * @param spark the {@link esiptestbed.mudrod.driver.SparkDriver} used to process input log files.
   */
  public ClickstreamImporter(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
    addClickStreamMapping();
  }

  /**
   * Method to add Elasticsearch mapping for click stream data
   */
  public void addClickStreamMapping(){
    XContentBuilder Mapping;
    try {
      Mapping = jsonBuilder()
          .startObject()
          .startObject(props.getProperty("clickstreamMatrixType"))
          .startObject("properties")
          .startObject("query")
          .field("type", "string")
          .field("index", "not_analyzed")
          .endObject()
          .startObject("dataID")
          .field("type", "string")
          .field("index", "not_analyzed")
          .endObject()

          .endObject()
          .endObject()
          .endObject();

      es.getClient().admin().indices()
      .preparePutMapping(props.getProperty("indexName"))
      .setType(props.getProperty("clickstreamMatrixType"))
      .setSource(Mapping)
      .execute().actionGet();
    } catch (IOException e) {
      e.printStackTrace();
    } 
  }

  /**
   * Method to import click stream CSV into Elasticsearch
   */
  public void importfromCSVtoES(){
    es.deleteType(props.getProperty("indexName"), props.getProperty("clickstreamMatrixType"));    
    es.createBulkProcesser();

    BufferedReader br = null;
    String cvsSplitBy = ",";

    try {
      br = new BufferedReader(new FileReader(props.getProperty("clickstreamMatrix")));
      String line = br.readLine();
      // first item needs to be skipped
      String dataList[] = line.split(cvsSplitBy);  
      while ((line = br.readLine()) != null) {
        String[] clicks = line.split(cvsSplitBy);
        for(int i=1; i<clicks.length; i++)
        {
          if(!clicks[i].equals("0.0"))
          {
            IndexRequest ir = new IndexRequest(props.getProperty("indexName"), props.getProperty("clickstreamMatrixType")).source(jsonBuilder()
                .startObject()
                .field("query", clicks[0])
                .field("dataID", dataList[i]) 
                .field("clicks", clicks[i])
                .endObject());
            es.getBulkProcessor().add(ir);
          }
        }
      }   
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (br != null) {
        try {
          br.close();
          es.destroyBulkProcessor();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

}