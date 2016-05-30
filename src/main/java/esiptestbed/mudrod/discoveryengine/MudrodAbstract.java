package esiptestbed.mudrod.discoveryengine;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import esiptestbed.mudrod.driver.ESDriver;

public abstract class MudrodAbstract {
	protected Map<String, String> config = new HashMap<String, String>();
	protected ESDriver es = null;
	public String HTTP_type = null;
	public String FTP_type = null;
	public String Cleanup_type = null;
	public String SessionStats = null;
	public final String settings_json = "{\r\n      \"analysis\": {\r\n         \"filter\": {\r\n            \"cody_stop\": {\r\n               \"type\": \"stop\",\r\n               \"stopwords\": \"_english_\"\r\n            },\r\n            \"cody_stemmer\": {\r\n               \"type\": \"stemmer\",\r\n               \"language\": \"light_english\"\r\n            }\r\n         },\r\n         \"analyzer\": {\r\n            \"cody\": {\r\n               \"tokenizer\": \"standard\",\r\n               \"filter\": [\r\n                  \"lowercase\",\r\n                  \"cody_stop\",\r\n                  \"cody_stemmer\"\r\n               ]\r\n            },\r\n            \"csv\": {\r\n               \"type\": \"pattern\",\r\n               \"pattern\": \",\"\r\n            }\r\n         }\r\n      }\r\n   }";
	public final String mapping_json = "{\r\n      \"_default_\": {\r\n         \"properties\": {\r\n            \"keywords\": {\r\n               \"type\": \"string\",\r\n               \"index_analyzer\": \"csv\"\r\n            },\r\n            \"views\": {\r\n               \"type\": \"string\",\r\n               \"index_analyzer\": \"csv\"\r\n            },\r\n            \"downloads\": {\r\n               \"type\": \"string\",\r\n               \"index_analyzer\": \"csv\"\r\n            },\r\n            \"RequestUrl\": {\r\n               \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"IP\": {\r\n               \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"Browser\": {\r\n               \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"SessionURL\": {\r\n               \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"Referer\": {\r\n               \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"SessionID\": {\r\n               \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"Response\": {\r\n               \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"Request\": {\r\n               \"type\": \"string\",\r\n               \"index\": \"not_analyzed\"\r\n            },\r\n            \"Coordinates\": {\r\n               \"type\": \"geo_point\"\r\n            }\r\n         }\r\n      }\r\n   }";
	
	
	public MudrodAbstract(Map<String, String> config, ESDriver es){
		this.config = config;
		this.es = es;
		try {
			es.putMapping(config.get("indexName"), settings_json, mapping_json);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		HTTP_type = config.get("HTTP_type");
		FTP_type = config.get("FTP_type");
		Cleanup_type = config.get("Cleanup_type");
		SessionStats = config.get("SessionStats");
	}
	
	public ESDriver getES(){
		return this.es;
	}
	
	public Map<String, String> getConfig(){
		return this.config;
	}
}
