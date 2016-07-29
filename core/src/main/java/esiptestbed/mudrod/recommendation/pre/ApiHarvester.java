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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.index.IndexRequest;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.utils.HttpRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApiHarvester extends DiscoveryStepAbstract {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(ApiHarvester.class);

  public ApiHarvester(Map<String, String> config, ESDriver es,
      SparkDriver spark) {
    super(config, es, spark);
  }

  @Override
  public Object execute() {
    LOG.info("*****************Metadata harvesting starts******************");
    startTime = System.currentTimeMillis();
    es.createBulkProcesser();
    addMetadataMapping();
    importToES();
    es.destroyBulkProcessor();
    endTime = System.currentTimeMillis();
    es.refreshIndex();
    LOG.info("*****************Metadata harvesting ends******************Took {}s", 
        (endTime - startTime) / 1000);
    return null;
  }

  public void addMetadataMapping() {
    String mappingJson = 
        "{\r\n   \"dynamic_templates\": "
            + "[\r\n      "
            + "{\r\n         \"strings\": "
            + "{\r\n            \"match_mapping_type\": \"string\","
            + "\r\n            \"mapping\": {\r\n               \"type\": \"string\","
            + "\r\n               \"analyzer\": \"csv\"\r\n            }"
            + "\r\n         }\r\n      }\r\n   ]\r\n}";
    es.client.admin().indices().preparePutMapping(config.get("indexName"))
    .setType(config.get("recom_metadataType")).setSource(mappingJson)
    .execute().actionGet();
  }

  private void importToES() {
    File directory = new File(config.get("raw_metadataPath"));
    File[] fList = directory.listFiles();
    for (File file : fList) {
      InputStream is;
      try {
        is = new FileInputStream(file);
        try {
          String jsonTxt = IOUtils.toString(is);
          JsonParser parser = new JsonParser();
          JsonElement item = parser.parse(jsonTxt);
          IndexRequest ir = new IndexRequest(config.get("indexName"),
              config.get("recom_metadataType")).source(item.toString());
          es.bulkProcessor.add(ir);
        } catch (IOException e) {
          e.printStackTrace();
        }
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      }

    }
  }

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

      File file = new File(config.get("raw_metadataPath"));
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
            config.get("raw_metadataPath") + "/" + docId + ".json");
        try {
          itemfile.createNewFile();
          FileWriter fw = new FileWriter(itemfile.getAbsoluteFile());
          BufferedWriter bw = new BufferedWriter(fw);
          bw.write(item.toString());
          bw.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }

      startIndex += 10;
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    } while (doc_length != 0);
  }

  @Override
  public Object execute(Object o) {
    // TODO Auto-generated method stub
    return null;
  }

}
