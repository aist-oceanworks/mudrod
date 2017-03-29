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
package gov.nasa.jpl.mudrod.ontology.process;

import gov.nasa.jpl.mudrod.discoveryengine.DiscoveryStepAbstract;
import gov.nasa.jpl.mudrod.driver.ESDriver;
import gov.nasa.jpl.mudrod.driver.SparkDriver;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Supports ability to parse and process FTP and HTTP log files
 */
public class OntologyLinkCal extends DiscoveryStepAbstract {

  public OntologyLinkCal(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
    es.deleteAllByQuery(props.getProperty("indexName"), props.getProperty("ontologyLinkageType"), QueryBuilders.matchAllQuery());
    addSWEETMapping();
  }

  /**
   * Method of adding mapping for triples extracted from SWEET
   */
  public void addSWEETMapping() {
    XContentBuilder Mapping;
    try {
      Mapping = jsonBuilder().startObject().startObject(props.getProperty("ontologyLinkageType")).startObject("properties").startObject("concept_A").field("type", "string")
          .field("index", "not_analyzed").endObject().startObject("concept_B").field("type", "string").field("index", "not_analyzed").endObject()

          .endObject().endObject().endObject();

      es.getClient().admin().indices().preparePutMapping(props.getProperty("indexName")).setType(props.getProperty("ontologyLinkageType")).setSource(Mapping).execute().actionGet();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Method of calculating and importing SWEET triples into Elasticsearch
   */
  @Override
  public Object execute() {
    es.deleteType(props.getProperty("indexName"), props.getProperty("ontologyLinkageType"));
    es.createBulkProcessor();

    BufferedReader br = null;
    String line = "";
    double weight = 0;

    try {
      br = new BufferedReader(new FileReader(props.getProperty("oceanTriples")));
      while ((line = br.readLine()) != null) {
        String[] strList = line.toLowerCase().split(",");
        if (strList[1].equals("subclassof")) {
          weight = 0.75;
        } else {
          weight = 0.9;
        }

        IndexRequest ir = new IndexRequest(props.getProperty("indexName"), props.getProperty("ontologyLinkageType")).source(
            jsonBuilder().startObject().field("concept_A", es.customAnalyzing(props.getProperty("indexName"), strList[2]))
                .field("concept_B", es.customAnalyzing(props.getProperty("indexName"), strList[0])).field("weight", weight).endObject());
        es.getBulkProcessor().add(ir);

      }

    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    } finally {
      if (br != null) {
        try {
          br.close();
          es.destroyBulkProcessor();
          es.refreshIndex();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    return null;
  }

  @Override
  public Object execute(Object o) {
    return null;
  }

}
