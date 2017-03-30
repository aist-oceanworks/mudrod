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

import gov.nasa.jpl.mudrod.discoveryengine.MudrodAbstract;
import gov.nasa.jpl.mudrod.driver.ESDriver;
import gov.nasa.jpl.mudrod.driver.SparkDriver;
import gov.nasa.jpl.mudrod.main.MudrodConstants;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Supports the ability to importing training set into Elasticsearch
 */
public class TrainingImporter extends MudrodAbstract {
  /**
   *
   */
  private static final long serialVersionUID = 1L;

  public TrainingImporter(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
    es.deleteAllByQuery(props.getProperty(MudrodConstants.ES_INDEX_NAME), "trainingranking", QueryBuilders.matchAllQuery());
    addMapping();
  }

  /**
   * Method of adding mapping to traning set type
   */
  public void addMapping() {
    XContentBuilder Mapping;
    try {
      Mapping = jsonBuilder().startObject().startObject("trainingranking").startObject("properties").startObject("query").field("type", "string").field("index", "not_analyzed").endObject()
          .startObject("dataID").field("type", "string").field("index", "not_analyzed").endObject().startObject("label").field("type", "string").field("index", "not_analyzed").endObject().endObject()
          .endObject().endObject();

      es.getClient().admin().indices().preparePutMapping(props.getProperty("indexName")).setType("trainingranking").setSource(Mapping).execute().actionGet();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Method of importing training set in to Elasticsearch
   *
   * @param dataFolder the path to the traing set
   * @throws IOException IOException
   */
  public void importTrainingSet(String dataFolder) throws IOException {
    es.createBulkProcessor();

    File[] files = new File(dataFolder).listFiles();
    for (File file : files) {
      BufferedReader br = new BufferedReader(new FileReader(file.getAbsolutePath()));
      br.readLine();
      String line = br.readLine();
      while (line != null) {
        String[] list = line.split(",");
        String query = file.getName().replace(".csv", "");
        if (list.length > 0) {
          IndexRequest ir = new IndexRequest(props.getProperty("indexName"), "trainingranking")
              .source(jsonBuilder().startObject().field("query", query).field("dataID", list[0]).field("label", list[list.length - 1]).endObject());
          es.getBulkProcessor().add(ir);
        }
        line = br.readLine();
      }
      br.close();
    }
    es.destroyBulkProcessor();
  }
}
