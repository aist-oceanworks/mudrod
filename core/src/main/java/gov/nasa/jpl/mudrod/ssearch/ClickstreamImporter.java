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
package gov.nasa.jpl.mudrod.ssearch;

import gov.nasa.jpl.mudrod.discoveryengine.MudrodAbstract;
import gov.nasa.jpl.mudrod.driver.ESDriver;
import gov.nasa.jpl.mudrod.driver.SparkDriver;
import gov.nasa.jpl.mudrod.main.MudrodConstants;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Supports ability to import click stream data into Elasticsearch
 * through .csv file
 */
public class ClickstreamImporter extends MudrodAbstract {
  /**
   *
   */
  private static final long serialVersionUID = 1L;

  public ClickstreamImporter(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
    addClickStreamMapping();
  }

  /**
   * Method to add Elasticsearch mapping for click stream data
   */
  public void addClickStreamMapping() {
    XContentBuilder Mapping;
    try {
      Mapping = jsonBuilder().startObject().startObject(
              props.getProperty(MudrodConstants.CLICK_STREAM_MATRIX_TYPE)).startObject(
                      "properties").startObject("query").field("type", "string").field(
                              "index", "not_analyzed").endObject().startObject("dataID").field(
                                      "type", "string").field("index", "not_analyzed").endObject()

          .endObject().endObject().endObject();

      es.getClient().admin().indices().preparePutMapping(
              props.getProperty(MudrodConstants.ES_INDEX_NAME)).setType(
                      props.getProperty(MudrodConstants.CLICK_STREAM_MATRIX_TYPE)).setSource(
                              Mapping).execute().actionGet();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Method to import click stream CSV into Elasticsearch
   */
  public void importfromCSVtoES() {
    es.deleteType(props.getProperty(MudrodConstants.ES_INDEX_NAME), 
            props.getProperty(MudrodConstants.CLICK_STREAM_MATRIX_TYPE));
    es.createBulkProcessor();

    BufferedReader br = null;
    String cvsSplitBy = ",";

    try {
      br = new BufferedReader(new FileReader(props.getProperty("clickstreamMatrix")));
      String line = br.readLine();
      // first item needs to be skipped
      String[] dataList = line.split(cvsSplitBy);
      while ((line = br.readLine()) != null) {
        String[] clicks = line.split(cvsSplitBy);
        for (int i = 1; i < clicks.length; i++) {
          if (!"0.0".equals(clicks[i])) {
            IndexRequest ir = new IndexRequest(props.getProperty(MudrodConstants.ES_INDEX_NAME), 
                    props.getProperty(MudrodConstants.CLICK_STREAM_MATRIX_TYPE))
                .source(jsonBuilder().startObject().field("query", clicks[0]).field(
                        "dataID", dataList[i]).field("clicks", clicks[i]).endObject());
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