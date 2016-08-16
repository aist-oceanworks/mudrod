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
package esiptestbed.mudrod.ontology.process;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;

/**
 * Extends {@link siptestbed.mudrod.discoveryengine.DiscoveryStepAbstract} to
 * add SWEET mappings for Ocean Triples obtained from the configuration <b>oceanTriples</b>
 * key.
 */
public class OntologyLinkCal extends DiscoveryStepAbstract {

  private static final Logger LOG = LoggerFactory.getLogger(OntologyLinkCal.class);

  private static final String INDEX_NAME = "indexName";

  private static final String ONT_TYPE = "ontologyLinkageType";

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  /**
   * 
   * @param config
   * @param es
   * @param spark
   */
  public OntologyLinkCal(Map<String, String> config, ESDriver es,
      SparkDriver spark) {
    super(config, es, spark);
    es.deleteAllByQuery(config.get(INDEX_NAME),
        config.get(ONT_TYPE), QueryBuilders.matchAllQuery());
    addSWEETMapping();
  }

  /**
   * Create SWEET configuration mapping for Elasticsearch.
   */
  public void addSWEETMapping() {
    XContentBuilder mapping;
    try {
      mapping = jsonBuilder().startObject()
          .startObject(config.get(ONT_TYPE))
          .startObject("properties").startObject("concept_A")
          .field("type", "string").field("index", "not_analyzed").endObject()
          .startObject("concept_B").field("type", "string")
          .field("index", "not_analyzed").endObject()

          .endObject().endObject().endObject();

      es.client.admin().indices().preparePutMapping(config.get(INDEX_NAME))
      .setType(config.get(ONT_TYPE)).setSource(mapping)
      .execute().actionGet();
    } catch (IOException e) {
      LOG.error("Error obtaining SWEET configuration mapping(s).", e);
    }
  }

  @Override
  public Object execute() {
    es.deleteType(config.get(INDEX_NAME), config.get(ONT_TYPE));
    es.createBulkProcesser();

    String line = "";
    double weight = 0;

    try (BufferedReader br = new BufferedReader(new FileReader(config.get("oceanTriples")))){
      while ((line = br.readLine()) != null) {
        String[] strList = line.toLowerCase().split(",");
        if ("subclassof".equals(strList[1])) {
          weight = 0.75;
        } else {
          weight = 0.9;
        }
        IndexRequest ir = new IndexRequest(config.get(INDEX_NAME),
            config.get(ONT_TYPE))
            .source(
                jsonBuilder().startObject()
                .field("concept_A",
                    es.customAnalyzing(config.get(INDEX_NAME),
                        strList[2]))
                .field("concept_B",
                    es.customAnalyzing(config.get(INDEX_NAME),
                        strList[0]))
                .field("weight", weight).endObject());
        es.bulkProcessor.add(ir);
      }
    } catch (IOException | InterruptedException | ExecutionException e) {
      LOG.error("Error executing index request.", e);
    } finally {
      es.destroyBulkProcessor();
      es.refreshIndex();
    }
    return null;
  }

  @Override
  public Object execute(Object o) {
    return null;
  }

}
