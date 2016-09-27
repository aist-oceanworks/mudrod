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
package esiptestbed.mudrod.recommendation.pre;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.index.IndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.metadata.pre.ApiHarvester;

/**
 * ClassName: Import Metadata to elasticsearch
 *
 */

public class ImportMetadata extends DiscoveryStepAbstract {

  /**
   *
   */
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(ApiHarvester.class);

  public ImportMetadata(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  @Override
  public Object execute() {
    LOG.info("*****************Metadata harvesting starts******************");
    startTime = System.currentTimeMillis();
    addMetadataMapping();
    importToES();
    endTime = System.currentTimeMillis();
    es.refreshIndex();
    LOG.info(
        "*****************Metadata harvesting ends******************Took {}s",
        (endTime - startTime) / 1000);
    return null;
  }

  /**
   * addMetadataMapping: Add mapping to index metadata in Elasticsearch. Please
   * invoke this method before import metadata to Elasticsearch.
   */
  public void addMetadataMapping() {
    String mappingJson = "{\r\n   \"dynamic_templates\": " + "[\r\n      "
        + "{\r\n         \"strings\": "
        + "{\r\n            \"match_mapping_type\": \"string\","
        + "\r\n            \"mapping\": {\r\n               \"type\": \"string\","
        + "\r\n               \"analyzer\": \"csv\"\r\n            }"
        + "\r\n         }\r\n      }\r\n   ]\r\n}";

    es.getClient().admin().indices()
        .preparePutMapping(props.getProperty("indexName"))
        .setType(props.getProperty("recom_metadataType")).setSource(mappingJson)
        .execute().actionGet();

  }

  /**
   * importToES: Index metadata into elasticsearch from local file directory.
   * Please make sure metadata have been harvest from web service before
   * invoking this method.
   */
  private void importToES() {
    es.deleteType(props.getProperty("indexName"),
        props.getProperty("recom_metadataType"));

    es.createBulkProcesser();
    File directory = new File(props.getProperty("raw_metadataPath"));
    File[] fList = directory.listFiles();
    for (File file : fList) {
      InputStream is;
      try {
        is = new FileInputStream(file);
        try {
          String jsonTxt = IOUtils.toString(is);
          JsonParser parser = new JsonParser();
          JsonElement item = parser.parse(jsonTxt);
          IndexRequest ir = new IndexRequest(props.getProperty("indexName"),
              props.getProperty("recom_metadataType")).source(item.toString());

          es.getBulkProcessor().add(ir);
        } catch (IOException e) {
          e.printStackTrace();
        }
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      }

    }

    es.destroyBulkProcessor();
  }

  @Override
  public Object execute(Object o) {
    return null;
  }
}
