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

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;

public class OntologyLinkCal extends DiscoveryStepAbstract {

	public OntologyLinkCal(Map<String, String> config, ESDriver es, SparkDriver spark) {
		super(config, es, spark);
		// TODO Auto-generated constructor stub
		es.deleteAllByQuery(config.get("indexName"), config.get("ontologyLinkageType"), QueryBuilders.matchAllQuery());
		addSWEETMapping();
	}
	
	 public void addSWEETMapping(){
	    	XContentBuilder Mapping;
			try {
				Mapping = jsonBuilder()
						.startObject()
							.startObject(config.get("ontologyLinkageType"))
								.startObject("properties")
									.startObject("concept_A")
										.field("type", "string")
										.field("index", "not_analyzed")
									.endObject()
									.startObject("concept_B")
										.field("type", "string")
										.field("index", "not_analyzed")
									.endObject()
									
								.endObject()
							.endObject()
						.endObject();
				
				es.client.admin().indices()
				  .preparePutMapping(config.get("indexName"))
		          .setType(config.get("ontologyLinkageType"))
		          .setSource(Mapping)
		          .execute().actionGet();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
	    }

	@Override
	public Object execute() {
		// TODO Auto-generated method stub
		es.deleteType(config.get("indexName"), config.get("ontologyLinkageType"));
		es.createBulkProcesser();		
		
		BufferedReader br = null;
		String line = "";
		double weight = 0;
		
		try {
			br = new BufferedReader(new FileReader(config.get("oceanTriples")));
			while ((line = br.readLine()) != null) {
				String[] strList = line.toLowerCase().split(",");
				if(strList[1].equals("subclassof"))
				{
					weight = 0.75;
				}else{
					weight = 0.9;
				}

				IndexRequest ir = new IndexRequest(config.get("indexName"), config.get("ontologyLinkageType")).source(jsonBuilder()
						.startObject()
						.field("concept_A", es.customAnalyzing(config.get("indexName"), strList[2]))
						.field("concept_B", es.customAnalyzing(config.get("indexName"), strList[0]))
						.field("weight", weight)	
						.endObject());
				es.bulkProcessor.add(ir);

			}

		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
					es.destroyBulkProcessor();
					es.refreshIndex();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return null;
	}

	@Override
	public Object execute(Object o) {
		// TODO Auto-generated method stub
		return null;
	}

}
