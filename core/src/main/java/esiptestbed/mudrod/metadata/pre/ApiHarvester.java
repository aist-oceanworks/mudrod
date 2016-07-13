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

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

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
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.utils.HttpRequest;

public class ApiHarvester extends DiscoveryStepAbstract {

  public ApiHarvester(Map<String, String> config, ESDriver es,
      SparkDriver spark) {
    super(config, es, spark);
    // TODO Auto-generated constructor stub
  }

  @Override
  public Object execute() {
    // TODO Auto-generated method stub
    System.out.println(
        "*****************Metadata harvesting starts******************");
    startTime = System.currentTimeMillis();
    es.createBulkProcesser();
    addMetadataMapping();
    // harvestMetadatafromWeb();
    importToES();
    es.destroyBulkProcessor();
    endTime = System.currentTimeMillis();
    es.refreshIndex();
    System.out.println(
        "*****************Metadata harvesting ends******************Took "
            + (endTime - startTime) / 1000 + "s");
    return null;
  }

  public void addMetadataMapping() {
    String mapping_json = "{\r\n   \"dynamic_templates\": [\r\n      {\r\n         \"strings\": {\r\n            \"match_mapping_type\": \"string\",\r\n            \"mapping\": {\r\n               \"type\": \"string\",\r\n               \"analyzer\": \"english\"\r\n            }\r\n         }\r\n      }\r\n   ]\r\n}";
    es.client.admin().indices().preparePutMapping(config.get("indexName"))
        .setType(config.get("raw_metadataType")).setSource(mapping_json)
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
              config.get("raw_metadataType")).source(item.toString());
          es.bulkProcessor.add(ir);
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      } catch (FileNotFoundException e) {
        // TODO Auto-generated catch block
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
          System.out.println("Directory is created!");
        } else {
          System.out.println("Failed to create directory!");
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
          // TODO Auto-generated catch block
          e.printStackTrace();
        }

        /*
         * IndexRequest ir = new IndexRequest(config.get("indexName"),
         * config.get("raw_metadataType")).source(item.toString());
         * es.bulkProcessor.add(ir);
         */
      }

      startIndex += 10;
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
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
