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

/**
 * This is the most generit class of MUDROD,
 * 
 * conig: configuration read from config.xml es: Elasticsearch instance
 * 
 */
public abstract class MudrodAbstract implements Serializable {
	protected Map<String, String> config = new HashMap<String, String>();
	protected ESDriver es = null;
	protected SparkDriver spark = null;
	protected long startTime, endTime;
	public String HTTP_type = null;
	public String FTP_type = null;
	public String Cleanup_type = null;
	public String SessionStats = null;
	//public final String settings_json = "{\r\n      \"analysis\": {\r\n         \"filter\": {\r\n            \"cody_stop\": {\r\n               \"type\": \"stop\",\r\n               \"stopwords\": \"_english_\"\r\n            },\r\n            \"cody_stemmer\": {\r\n               \"type\": \"stemmer\",\r\n               \"language\": \"light_english\"\r\n            }\r\n         },\r\n         \"analyzer\": {\r\n            \"cody\": {\r\n               \"tokenizer\": \"standard\",\r\n               \"filter\": [\r\n                  \"lowercase\",\r\n                  \"cody_stop\",\r\n                  \"cody_stemmer\"\r\n               ]\r\n            },\r\n            \"csv\": {\r\n               \"type\": \"pattern\",\r\n               \"pattern\": \",\"\r\n            }\r\n         }\r\n      }\r\n   }";
	
	public final String settings_json = "{\r\n   \"index\": {\r\n      \"number_of_replicas\": 0,\r\n      \"refresh_interval\": \"-1\"\r\n   },\r\n   \"analysis\": {\r\n      \"filter\": {\r\n         \"cody_stop\": {\r\n            \"type\": \"stop\",\r\n            \"stopwords\": \"_english_\"\r\n         },\r\n         \"cody_stemmer\": {\r\n            \"type\": \"stemmer\",\r\n            \"language\": \"light_english\"\r\n         }\r\n      },\r\n      \"analyzer\": {\r\n         \"cody\": {\r\n            \"tokenizer\": \"standard\",\r\n            \"filter\": [\r\n               \"lowercase\",\r\n               \"cody_stop\",\r\n               \"cody_stemmer\"\r\n            ]\r\n         },\r\n         \"csv\": {\r\n            \"type\": \"pattern\",\r\n            \"pattern\": \",\"\r\n         }\r\n      }\r\n   }\r\n}";
	public final String mapping_json = "{\r\n      \"_default_\": {\r\n         \"properties\": {\r\n            \"keywords\": {\r\n               \"type\": \"string\",\r\n               \"index_analyzer\": \"csv\"\r\n            },\r\n            \"views\": {\r\n               \"type\": \"string\",\r\n               \"index_analyzer\": \"csv\"\r\n            },\r\n            \"downloads\": {\r\n               \"type\": \"string\",\r\n               \"index_analyzer\": \"csv\"\r\n            },\r\n            \"RequestUrl\": {\r\n               \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"IP\": {\r\n               \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"Browser\": {\r\n               \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"SessionURL\": {\r\n               \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"Referer\": {\r\n               \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"SessionID\": {\r\n               \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"Response\": {\r\n               \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"Request\": {\r\n               \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"Coordinates\": {\r\n               \"type\": \"geo_point\"\r\n            }\r\n         }\r\n      }\r\n   }";
	//public final String mapping_json = "{\r\n   \"mappings\": {\r\n      \"RawMetadata\": {\r\n         \"dynamic_templates\": [\r\n            {\r\n               \"strings\": {\r\n                  \"match_mapping_type\": \"string\",\r\n                  \"mapping\": {\r\n                     \"type\": \"string\",\r\n                     \"index_analyzer\": \"cody\"\r\n                  }\r\n               }\r\n            }\r\n         ]\r\n      },\r\n      \"_default_\": {\r\n         \"properties\": {\r\n            \"keywords\": {\r\n               \"type\": \"string\",\r\n               \"index_analyzer\": \"csv\"\r\n            },\r\n            \"views\": {\r\n               \"type\": \"string\",\r\n               \"index_analyzer\": \"csv\"\r\n            },\r\n            \"downloads\": {\r\n               \"type\": \"string\",\r\n               \"index_analyzer\": \"csv\"\r\n            },\r\n            \"RequestUrl\": {\r\n               \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"IP\": {\r\n               \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"Browser\": {\r\n               \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"SessionURL\": {\r\n               \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"Referer\": {\r\n               \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"SessionID\": {\r\n               \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"Response\": {\r\n               \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"Request\": {\r\n               \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"Coordinates\": {\r\n               \"type\": \"geo_point\"\r\n            }\r\n         }\r\n      }\r\n   }\r\n}";
	private int print_level = 0;

	public MudrodAbstract(Map<String, String> config, ESDriver es, SparkDriver spark){
		this.config = config;
		this.es = es;
		this.spark = spark;
		this.initMudrod();
	}

	protected void initMudrod(){
		try {
			es.putMapping(config.get("indexName"), settings_json, mapping_json);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		HTTP_type = config.get("HTTP_type_prefix");
		FTP_type = config.get("FTP_type_prefix");
		Cleanup_type = config.get("Cleanup_type_prefix");
		SessionStats = config.get("SessionStats_prefix");

		print_level = Integer.parseInt(config.get("loglevel"));
	}

	public void print(String log, int level){
		if(level <= print_level)
		{
			System.out.println(log);
		}
	}

	public ESDriver getES(){
		return this.es;
	}

	public Map<String, String> getConfig(){
		return this.config;
	}
}
