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
package esiptestbed.mudrod.metadata.pre;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.index.IndexRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.main.MudrodConstants;
import esiptestbed.mudrod.utils.HttpRequest;

/**
 * ClassName: ApiHarvester Function: Harvest metadata from PO.DAACweb service.
 */
public class ApiHarvester extends DiscoveryStepAbstract {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(ApiHarvester.class);

  /**
   * Creates a new instance of ApiHarvester.
   *
   * @param props
   *          the Mudrod configuration
   * @param es
   *          the Elasticsearch drive
   * @param spark
   *          the spark driver
   */
  public ApiHarvester(Properties props, ESDriver es,
      SparkDriver spark) {
    super(props, es, spark);
  }

  @Override
  public Object execute() {
    LOG.info("Starting Metadata harvesting.");
    startTime = System.currentTimeMillis();
    es.createBulkProcessor();
    addMetadataMapping();
    importToES();
    es.destroyBulkProcessor();
    endTime = System.currentTimeMillis();
    es.refreshIndex();
    LOG.info("Metadata harvesting completed. Time elapsed: {}",
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
        + "\r\n               \"analyzer\": \"english\"\r\n            }"
        + "\r\n         }\r\n      }\r\n   ]\r\n}";
    es.getClient().admin().indices().preparePutMapping(props.getProperty("indexName"))
    .setType(props.getProperty("raw_metadataType")).setSource(mappingJson)
    .execute().actionGet();
  }

  /**
   * importToES: Index metadata into elasticsearch from local file directory.
   * Please make sure metadata have been harvest from web service before
   * invoking this method.
   */
  private void importToES() {
    File directory = new File(props.getProperty(MudrodConstants.RAW_METADATA_PATH));
    File[] fList = directory.listFiles();
    for (File file : fList) {
      InputStream is;
      try {
        is = new FileInputStream(file);
        importSingleFileToES(is);
      } catch (FileNotFoundException e) {
        LOG.error("Error finding file!", e);
      }

    }
  }
  
  private void importSingleFileToES(InputStream is)
  {
    try {
      String jsonTxt = IOUtils.toString(is);
      JsonParser parser = new JsonParser();
      JsonElement item = parser.parse(jsonTxt);
      IndexRequest ir = new IndexRequest(props.getProperty("indexName"),
          props.getProperty("raw_metadataType")).source(item.toString());
      es.getBulkProcessor().add(ir);
    } catch (IOException e) {
      LOG.error("Error indexing metadata record!", e);
    }
  }

  /**
   * harvestMetadatafromWeb: Harvest metadata from PO.DAAC web service.
   */
  private void harvestMetadatafromWeb() {
    int startIndex = 0;
    int doc_length = 0;
    JsonParser parser = new JsonParser();
    do {
      String searchAPI = "https://podaac.jpl.nasa.gov/api/dataset?startIndex="
          + Integer.toString(startIndex)
          + "&entries=10&sortField=Dataset-AllTimePopularity&sortOrder=asc&id=&value=&search=";
      HttpRequest http = new HttpRequest();
      String response = http.getRequest(searchAPI);

      JsonElement json = parser.parse(response);
      JsonObject responseObject = json.getAsJsonObject();
      JsonArray docs = responseObject.getAsJsonObject("response")
          .getAsJsonArray("docs");

      doc_length = docs.size();

      File file = new File(props.getProperty(MudrodConstants.RAW_METADATA_PATH));
      if (!file.exists()) {
        if (file.mkdir()) {
          LOG.info("Directory is created!");
        } else {
          LOG.error("Failed to create directory!");
        }
      }
      for (int i = 0; i < doc_length; i++) {
        JsonElement item = docs.get(i);
        int docId = startIndex + i;
        File itemfile = new File(
            props.getProperty(MudrodConstants.RAW_METADATA_PATH) + "/" + docId + ".json");
        
        try (FileWriter fw = new FileWriter(itemfile.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);){
          itemfile.createNewFile();
          bw.write(item.toString());
        } catch (IOException e) {
          LOG.error("Error writing metadata to local file!", e);
        }
      }

      startIndex += 10;

      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        LOG.error("Error entering Elasticsearch Mappings!", e);
        Thread.currentThread().interrupt();
      }

    } while (doc_length != 0);
  }

  @Override
  public Object execute(Object o) {
    return null;
  }

}
