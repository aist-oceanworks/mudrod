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
package esiptestbed.mudrod.driver;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.IOException;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse.AnalyzeToken;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.suggest.SuggestRequestBuilder;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionFuzzyBuilder;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import esiptestbed.mudrod.integration.LinkageIntegration;



public class ESDriver implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public Client client = null;
	public Node node = null;
	public BulkProcessor bulkProcessor = null;

	public ESDriver(String clusterName){
		node =
				nodeBuilder()
				.settings(ImmutableSettings.settingsBuilder().put("http.enabled", false))
				.clusterName(clusterName)
				.client(true)
				.node();

		client = node.client();	
	}

	public void createBulkProcesser(){
		bulkProcessor = BulkProcessor.builder(
				client,
				new BulkProcessor.Listener() {
					public void beforeBulk(long executionId,
							BulkRequest request) {/*System.out.println("New request!");*/} 

					public void afterBulk(long executionId,
							BulkRequest request,
							BulkResponse response) {/*System.out.println("Well done!");*/} 

					public void afterBulk(long executionId,
							BulkRequest request,
							Throwable failure) {
						System.out.println("Bulk fails!");
						throw new RuntimeException("Caught exception in bulk: " + request + ", failure: " + failure, failure);
					} 
				}
				)
				.setBulkActions(1000) 
				.setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB)) 
				//.setFlushInterval(TimeValue.timeValueSeconds(5))    //let's test this
				.setConcurrentRequests(1) 
				.build();
	}

	public void destroyBulkProcessor(){
		try {
			bulkProcessor.awaitClose(20, TimeUnit.MINUTES);
			bulkProcessor = null;
			refreshIndex();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void putMapping(String indexName, String settings_json, String mapping_json) throws IOException{

		boolean exists = client.admin().indices().prepareExists(indexName).execute().actionGet().isExists();
		if(exists){
			return;
		}

		client.admin().indices().prepareCreate(indexName).setSettings(ImmutableSettings.settingsBuilder().loadFromSource(settings_json)).execute().actionGet();
		client.admin().indices()
		.preparePutMapping(indexName)
		.setType("_default_")				            
		.setSource(mapping_json)
		.execute().actionGet();
	}

	public String customAnalyzing(String indexName, String str) throws InterruptedException, ExecutionException{
		String[] str_list = str.toLowerCase().split(",");
		for(int i = 0; i<str_list.length;i++)
		{
			String tmp = "";
			AnalyzeResponse r = client.admin().indices()
					.prepareAnalyze(str_list[i]).setIndex(indexName).setAnalyzer("cody")
					.execute().get();
			for (AnalyzeToken token : r.getTokens()) {
				tmp +=token.getTerm() + " ";
			}
			str_list[i] = tmp.trim();
		}

		String analyzed_str = String.join(",", str_list);
		return analyzed_str;
	}

	public List<String> customAnalyzing(String indexName, List<String> list) throws InterruptedException, ExecutionException{
		if(list == null){
			return list;
		}
		int size = list.size();
		List<String> customlist = new ArrayList<String>();
		for(int i=0; i<size; i++){
			customlist.add(this.customAnalyzing(indexName, list.get(i)));
		}

		return customlist;
	}

	public void deleteAllByQuery(String index, String type, QueryBuilder query) {
		createBulkProcesser();
		SearchResponse scrollResp = client.prepareSearch(index)
				.setSearchType(SearchType.SCAN)
				.setTypes(type)
				.setScroll(new TimeValue(60000))
				.setQuery(query)
				.setSize(10000)
				.execute().actionGet();  //10000 hits per shard will be returned for each scroll

		while (true) {
			for (SearchHit hit : scrollResp.getHits().getHits()) {
				DeleteRequest deleteRequest = new DeleteRequest(index, type, hit.getId());
				bulkProcessor.add(deleteRequest);
			}

			System.out.println("Need to delete " + scrollResp.getHits().getHits().length + " records");

			scrollResp = client.prepareSearchScroll(scrollResp.getScrollId())
					.setScroll(new TimeValue(600000)).execute().actionGet();
			if (scrollResp.getHits().getHits().length == 0) {
				break;
			}

		}
		destroyBulkProcessor();
	}

	public void deleteType(String index, String type){
		this.deleteAllByQuery(index, type, QueryBuilders.matchAllQuery());
	}

	public ArrayList<String> getTypeListWithPrefix(String index, String type_prefix)
	{
		ArrayList<String> type_list = new ArrayList<String>();
		GetMappingsResponse res;
		try {
			res = client.admin().indices().getMappings(new GetMappingsRequest().indices(index)).get();
			ImmutableOpenMap<String, MappingMetaData> mapping  = res.mappings().get(index);
			for (ObjectObjectCursor<String, MappingMetaData> c : mapping) {
				//System.out.println(c.key+" = "+c.value.source());
				if(c.key.startsWith(type_prefix))
				{
					type_list.add(c.key);
				}
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return type_list;	    
	}

	public String searchByQuery(String index, String Type, String query) throws IOException, InterruptedException, ExecutionException{
		boolean exists = node.client().admin().indices().prepareExists(index).execute().actionGet().isExists();	
		if(!exists){
			return null;
		}

		QueryBuilder qb = QueryBuilders.queryStringQuery(query); 
		SearchResponse response = client.prepareSearch(index)
				.setTypes(Type)		        
				.setQuery(qb)
				.setSize(500)
				.execute()
				.actionGet();

		Gson gson = new Gson();		
		List<JsonObject> fileList = new ArrayList<JsonObject>();
		DecimalFormat twoDForm = new DecimalFormat("#.##");
		
		for (SearchHit hit : response.getHits().getHits()) {
			Map<String,Object> result = hit.getSource();
			Double relevance = Double.valueOf(twoDForm.format(hit.getScore()));
			String shortName = (String) result.get("Dataset-ShortName");
			String longName = (String) result.get("Dataset-LongName");
			@SuppressWarnings("unchecked")
			ArrayList<String> topicList = (ArrayList<String>) result.get("DatasetParameter-Variable");
			String topic = String.join(", ", topicList);
			String content = (String) result.get("Dataset-Description");
			@SuppressWarnings("unchecked")
			ArrayList<String> longdate = (ArrayList<String>) result.get("DatasetCitation-ReleaseDateLong");
			
			Date date=new Date(Long.valueOf(longdate.get(0)).longValue());
	        SimpleDateFormat df2 = new SimpleDateFormat("dd/MM/yy");
	        String dateText = df2.format(date);

			JsonObject file = new JsonObject();
			file.addProperty("Relevance", relevance);
			file.addProperty("Short Name", shortName);
			file.addProperty("Long Name", longName);
			file.addProperty("Topic", topic);
			file.addProperty("Abstract", content);
			file.addProperty("Release Date", dateText);
			fileList.add(file);       	

		}
		JsonElement fileList_Element = gson.toJsonTree(fileList);

		JsonObject PDResults = new JsonObject();
		PDResults.add("PDResults", fileList_Element);
		System.out.print("Search results returned." + "\n");
		return PDResults.toString();
	}

	public List<String> autoComplete(String index, String chars){
		boolean exists = node.client().admin().indices().prepareExists(index).execute().actionGet().isExists();	
		if(!exists){
			return null;
		}

		List<String> SuggestList = new ArrayList<String>();

		CompletionSuggestionFuzzyBuilder suggestionsBuilder = new CompletionSuggestionFuzzyBuilder("completeMe");
		suggestionsBuilder.text(chars);
		suggestionsBuilder.size(10);
		suggestionsBuilder.field("name_suggest");
		suggestionsBuilder.setFuzziness(Fuzziness.fromEdits(2));  

		SuggestRequestBuilder suggestRequestBuilder =
				client.prepareSuggest(index).addSuggestion(suggestionsBuilder);


		SuggestResponse suggestResponse = suggestRequestBuilder.execute().actionGet();

		Iterator<? extends Suggest.Suggestion.Entry.Option> iterator =
				suggestResponse.getSuggest().getSuggestion("completeMe").iterator().next().getOptions().iterator();

		while (iterator.hasNext()) {
			Suggest.Suggestion.Entry.Option next = iterator.next();
			SuggestList.add(next.getText().string());
		}
		return SuggestList;

	}

	public void close(){
		node.close();   
	}

	public void refreshIndex(){
		node.client().admin().indices().prepareRefresh().execute().actionGet();
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ESDriver es = new ESDriver("cody");
		es.getTypeListWithPrefix("podaacsession", "sessionstats");
	}

}
