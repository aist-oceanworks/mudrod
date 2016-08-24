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
import java.util.Properties;

import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the most generic class of Mudrod
 */
public abstract class MudrodAbstract implements Serializable {
  
  private static final Logger LOG = LoggerFactory.getLogger(MudrodAbstract.class);
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private static final String TIME_SUFFIX = "TimeSuffix";
  protected Properties props = new Properties();
  protected ESDriver es = null;
  protected SparkDriver spark = null;
  protected long startTime;
  protected long endTime;
  public String httpType = null;
  public String ftpType = null;
  public String cleanupType = null;
  public String sessionStats = null;

  public final String settingsJson = "{\r\n   \"index\": {\r\n      \"number_of_replicas\": 0,\r\n      \"refresh_interval\": \"-1\"\r\n   },\r\n   \"analysis\": {\r\n      \"filter\": {\r\n         \"cody_stop\": {\r\n            \"type\": \"stop\",\r\n            \"stopwords\": \"_english_\"\r\n         },\r\n         \"cody_stemmer\": {\r\n            \"type\": \"stemmer\",\r\n            \"language\": \"light_english\"\r\n         }\r\n      },\r\n      \"analyzer\": {\r\n         \"cody\": {\r\n            \"tokenizer\": \"standard\",\r\n            \"filter\": [\r\n               \"lowercase\",\r\n               \"cody_stop\",\r\n               \"cody_stemmer\"\r\n            ]\r\n         },\r\n         \"csv\": {\r\n            \"type\": \"pattern\",\r\n            \"pattern\": \",\"\r\n         }\r\n      }\r\n   }\r\n}";
  public final String mappingJson = "{\r\n      \"_default_\": {\r\n         \"properties\": {\r\n            \"keywords\": {\r\n               \"type\": \"string\",\r\n               \"index_analyzer\": \"csv\"\r\n            },\r\n            \"views\": {\r\n               \"type\": \"string\",\r\n               \"index_analyzer\": \"csv\"\r\n            },\r\n            \"downloads\": {\r\n               \"type\": \"string\",\r\n               \"index_analyzer\": \"csv\"\r\n            },\r\n            \"RequestUrl\": {\r\n               \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"IP\": {\r\n               \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"Browser\": {\r\n               \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"SessionURL\": {\r\n               \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"Referer\": {\r\n               \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"SessionID\": {\r\n               \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"Response\": {\r\n               \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"Request\": {\r\n               \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"Coordinates\": {\r\n               \"type\": \"geo_point\"\r\n            }\r\n         }\r\n      }\r\n   }";
  private int printLevel = 0;

  public MudrodAbstract(Properties props, ESDriver es, SparkDriver spark){
    this.props = props;
    this.es = es;
    this.spark = spark;
    this.initMudrod();
  }

  protected void initMudrod(){
    try {
      this.es.putMapping(props.getProperty("indexName"), settingsJson, mappingJson);
    } catch (IOException e) {
      LOG.error("Error entering Elasticsearch Mappings!", e);
    }

    httpType = props.getProperty("HTTP_type_prefix") + props.getProperty(TIME_SUFFIX);
    ftpType = props.getProperty("FTP_type_prefix") + props.getProperty(TIME_SUFFIX);
    cleanupType = props.getProperty("Cleanup_type_prefix") + props.getProperty(TIME_SUFFIX);
    sessionStats = props.getProperty("SessionStats_prefix") + props.getProperty(TIME_SUFFIX);

    printLevel = Integer.parseInt(props.getProperty("loglevel"));
  }

  public void print(String log, int level){
    if(level <= printLevel)
    {
      LOG.info(log);
    }
  }

  public ESDriver getES(){
    return this.es;
  }

  public Properties getConfig(){
    return this.props;
  }
}
