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

import java.util.Map;

import org.elasticsearch.action.index.IndexRequest;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.utils.HttpRequest;

public class ApiHarvester extends DiscoveryStepAbstract {

	public ApiHarvester(Map<String, String> config, ESDriver es, SparkDriver spark) {
		super(config, es, spark);
		// TODO Auto-generated constructor stub
	}

	@Override
	public Object execute() {
		// TODO Auto-generated method stub
		System.out.println("*****************Metadata harvesting starts******************");
		startTime=System.currentTimeMillis();
		es.createBulkProcesser();
		getMetadata();
		es.destroyBulkProcessor();
		endTime=System.currentTimeMillis();
		es.refreshIndex();
		System.out.println("*****************Metadata harvesting ends******************Took " + (endTime-startTime)/1000+"s");
		return null;
	}
	
	private void getMetadata()
	{
		int startIndex = 0;
		int doc_length = 0;
		JsonParser parser = new JsonParser();
		do{
			String searchAPI="https://podaac.jpl.nasa.gov/api/dataset?startIndex=" + Integer.toString(startIndex) +"&entries=10&sortField=Dataset-AllTimePopularity&sortOrder=asc&id=&value=&search=";
			HttpRequest http = new HttpRequest();		
			String response = http.getRequest(searchAPI);	

		    JsonElement json = parser.parse(response);
		    JsonObject responseObject = json.getAsJsonObject();
		    JsonArray docs = responseObject.getAsJsonObject("response").getAsJsonArray("docs");
		    
		    doc_length = docs.size();
		    for(int i =0; i < doc_length; i++)
		    {
		    	JsonElement item = docs.get(i);
		    	IndexRequest ir = new IndexRequest(config.get("indexName"), config.get("raw_metadataType")).source(item.toString());

				es.bulkProcessor.add(ir);
		    }
		    startIndex +=10;
		}while(doc_length!=0);
	}

	@Override
	public Object execute(Object o) {
		// TODO Auto-generated method stub
		return null;
	}

}
