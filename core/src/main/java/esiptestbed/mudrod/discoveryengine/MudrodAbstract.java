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
package esiptestbed.mudrod.discoveryengine;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Superclass of mudrod program
 * @author Yongyao
 *
 */
public abstract class MudrodAbstract implements Serializable {
  
  private static final Logger LOG = LoggerFactory.getLogger(MudrodAbstract.class);
  private static final long serialVersionUID = 1L;
  protected Map<String, String> config = new HashMap<>();
  protected ESDriver es = null;
  protected SparkDriver spark = null;
  protected long startTime, endTime;
  public String HTTP_type = null;
  public String FTP_type = null;
  public String Cleanup_type = null;
  public String SessionStats = null;

  public final String settingsJson = "{\r\n   \"index\": {\r\n      \"number_of_replicas\": 0,\r\n      \"refresh_interval\": \"-1\"\r\n   },\r\n   \"analysis\": {\r\n      \"filter\": {\r\n         \"cody_stop\": {\r\n            \"type\": \"stop\",\r\n            \"stopwords\": \"_english_\"\r\n         },\r\n         \"cody_stemmer\": {\r\n            \"type\": \"stemmer\",\r\n            \"language\": \"light_english\"\r\n         }\r\n      },\r\n      \"analyzer\": {\r\n         \"cody\": {\r\n            \"tokenizer\": \"standard\",\r\n            \"filter\": [\r\n               \"lowercase\",\r\n               \"cody_stop\",\r\n               \"cody_stemmer\"\r\n            ]\r\n         },\r\n         \"csv\": {\r\n            \"type\": \"pattern\",\r\n            \"pattern\": \",\"\r\n         }\r\n      }\r\n   }\r\n}";
  public final String mappingJson = "{\r\n      \"_default_\": {\r\n         \"properties\": {\r\n            \"keywords\": {\r\n               \"type\": \"string\",\r\n               \"index_analyzer\": \"csv\"\r\n            },\r\n            \"views\": {\r\n               \"type\": \"string\",\r\n               \"index_analyzer\": \"csv\"\r\n            },\r\n            \"downloads\": {\r\n               \"type\": \"string\",\r\n               \"index_analyzer\": \"csv\"\r\n            },\r\n            \"RequestUrl\": {\r\n               \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"IP\": {\r\n               \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"Browser\": {\r\n               \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"SessionURL\": {\r\n               \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"Referer\": {\r\n               \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"SessionID\": {\r\n               \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"Response\": {\r\n               \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"Request\": {\r\n               \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"Coordinates\": {\r\n               \"type\": \"geo_point\"\r\n            }\r\n         }\r\n      }\r\n   }";
/**
 * Constructuor method
 * @param config  configuration
 * @param es      elasticsearch client
 * @param spark   spark connection
 */
  public MudrodAbstract(Map<String, String> config, ESDriver es, SparkDriver spark){
    this.config = config;
    this.es = es;
    this.spark = spark;
    this.initMudrod();
  }

  protected void initMudrod(){
    try {
      es.putMapping(config.get("indexName"), settingsJson, mappingJson);
    } catch (IOException e) {
      e.printStackTrace();
    }

    HTTP_type = config.get("HTTP_type_prefix") + config.get("TimeSuffix");
    FTP_type = config.get("FTP_type_prefix") + config.get("TimeSuffix");
    Cleanup_type = config.get("Cleanup_type_prefix") + config.get("TimeSuffix");
    SessionStats = config.get("SessionStats_prefix") + config.get("TimeSuffix");
  }

  public ESDriver getES(){
    return this.es;
  }

  public Map<String, String> getConfig(){
    return this.config;
  }
}
