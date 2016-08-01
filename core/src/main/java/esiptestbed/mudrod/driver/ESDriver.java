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

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
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

import org.elasticsearch.ElasticsearchException;
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
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionFuzzyBuilder;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ESDriver implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(ESDriver.class);
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

  public ESDriver(Map<String, String> config){
    Settings settings = System.getProperty("file.separator").equals("/") ? ImmutableSettings.settingsBuilder()
        .put("http.enabled", "false")
        .put("transport.tcp.port", config.get("ES_Transport_TCP_Port"))
        .put("discovery.zen.ping.multicast.enabled", "false")
        .put("discovery.zen.ping.unicast.hosts", config.get("ES_unicast_hosts"))
        .build() : ImmutableSettings.settingsBuilder()
        .put("http.enabled", false)
        .build();;

        node =
            nodeBuilder()
            .settings(settings)
            .clusterName(config.get("clusterName"))
            .client(true)
            .node();
        client = node.client();
  }

  public void createBulkProcesser(){
    bulkProcessor = BulkProcessor.builder(
        client,
        new BulkProcessor.Listener() {
          public void beforeBulk(long executionId,
              BulkRequest request) {} 

          public void afterBulk(long executionId,
              BulkRequest request,
              BulkResponse response) {} 

          public void afterBulk(long executionId,
              BulkRequest request,
              Throwable failure) {
            LOG.error("Bulk request has failed!");
            throw new RuntimeException("Caught exception in bulk: " + request + ", failure: " + failure, failure);
          } 
        }
        )
        .setBulkActions(1000) 
        .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB)) 
        .setConcurrentRequests(1) 
        .build();
  }

  public void destroyBulkProcessor(){
    try {
      bulkProcessor.awaitClose(20, TimeUnit.MINUTES);
      bulkProcessor = null;
      refreshIndex();
    } catch (InterruptedException e) {
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
    String[] strList = str.toLowerCase().split(",");
    for(int i = 0; i<strList.length;i++)
    {
      String tmp = "";
      AnalyzeResponse r = client.admin().indices()
          .prepareAnalyze(strList[i]).setIndex(indexName).setAnalyzer("cody")
          .execute().get();
      for (AnalyzeToken token : r.getTokens()) {
        tmp +=token.getTerm() + " ";
      }
      strList[i] = tmp.trim();
    }
    return String.join(",", strList);
  }
  
  public String customAnalyzing(String indexName, String analyzer, String str) throws InterruptedException, ExecutionException{
	    String[] strList = str.toLowerCase().split(",");
	    for(int i = 0; i<strList.length;i++)
	    {
	      String tmp = "";
	      AnalyzeResponse r = client.admin().indices()
	          .prepareAnalyze(strList[i]).setIndex(indexName).setAnalyzer(analyzer)
	          .execute().get();
	      for (AnalyzeToken token : r.getTokens()) {
	        tmp +=token.getTerm() + " ";
	      }
	      strList[i] = tmp.trim();
	    }
	    return String.join(",", strList);
  }

  public List<String> customAnalyzing(String indexName, List<String> list) throws InterruptedException, ExecutionException{
    if(list == null){
      return list;
    }
    int size = list.size();
    List<String> customlist = new ArrayList<>();
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
        .execute().actionGet();

    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        DeleteRequest deleteRequest = new DeleteRequest(index, type, hit.getId());
        bulkProcessor.add(deleteRequest);
      }

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

  public ArrayList<String> getTypeListWithPrefix(String index, String typePrefix)
  {
    ArrayList<String> typeList = new ArrayList<>();
    GetMappingsResponse res;
    try {
      res = client.admin().indices().getMappings(new GetMappingsRequest().indices(index)).get();
      ImmutableOpenMap<String, MappingMetaData> mapping  = res.mappings().get(index);
      for (ObjectObjectCursor<String, MappingMetaData> c : mapping) {
        if(c.key.startsWith(typePrefix))
        {
          typeList.add(c.key);
        }
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
    return typeList;
  }

  public String searchByQuery(String index, String Type, String query) throws IOException, InterruptedException, ExecutionException{
	  return searchByQuery(index, Type, query, false);
  }
  
  public String searchByQuery(String index, String Type, String query, boolean bdetail) throws IOException, InterruptedException, ExecutionException{
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
	    List<JsonObject> fileList = new ArrayList<>();
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
	      
	      if(bdetail){
	    	  file.addProperty("Processing Level", (String) result.get("Dataset-ProcessingLevel"));
		      file.addProperty("Dataset-Doi", (String) result.get("Dataset-Doi"));
		      file.addProperty("Dataset-TemporalRepeat", (String) result.get("Dataset-TemporalRepeat"));
		      file.addProperty("Dataset-TemporalRepeatMax", (String) result.get("Dataset-TemporalRepeatMax"));
		      file.addProperty("Dataset-TemporalRepeatMin", (String) result.get("Dataset-TemporalRepeatMin"));
		      file.addProperty("DatasetPolicy-DataFormat", (String) result.get(" DatasetPolicy-DataFormat"));
		      file.addProperty("DatasetPolicy-DataLatency", (String) result.get("DatasetPolicy-DataLatency"));
		      file.addProperty("Dataset-Description", (String) result.get("Dataset-Description"));

		      List<String> sensors = (List<String>) result.get("DatasetSource-Sensor-ShortName");
		      file.addProperty("DatasetSource-Sensor-ShortName", String.join(", ", sensors));  
		     
		      List<String> projects = (List<String>) result.get("DatasetProject-Project-ShortName");
		      file.addProperty("DatasetProject-Project-ShortName", String.join(", ", projects));  
		      
		      List<String> categories = (List<String>) result.get("DatasetParameter-Category");
		      file.addProperty("DatasetParameter-Category", String.join(", ", categories));  
		      
		      List<String> variables = (List<String>) result.get("DatasetParameter-Variable");
		      file.addProperty("DatasetParameter-Variable", String.join(", ", variables));  
		      
		      List<String> terms = (List<String>) result.get("DatasetParameter-Term");
		      file.addProperty("DatasetParameter-Term", String.join(", ", terms));  
	      
		      
		     
		      
		      
		     
		      
	      }
	      
	      fileList.add(file);       	

	    }
	    JsonElement fileListElement = gson.toJsonTree(fileList);

	    JsonObject PDResults = new JsonObject();
	    PDResults.add("PDResults", fileListElement);
	    LOG.info("Search results returned. \n");
	    return PDResults.toString();
	  }

  public List<String> autoComplete(String index, String chars){
    boolean exists = node.client().admin().indices().prepareExists(index).execute().actionGet().isExists();	
    if(!exists){
      return null;
    }

    List<String> suggestList = new ArrayList<>();

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
      suggestList.add(next.getText().string());
    }
    return suggestList;

  }

  public void close(){
    node.close();
  }

  public void refreshIndex(){
    node.client().admin().indices().prepareRefresh().execute().actionGet();
  }
  
  public UpdateRequest genUpdateRequest(String index, String type, String id, String field1,
	      Object value1) {
	  
	    UpdateRequest ur = null;
		try {
			ur = new UpdateRequest(index, type, id).doc(jsonBuilder().startObject().field(field1, value1).endObject());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	    return ur;
  }
  
  public UpdateRequest genUpdateRequest(String index, String type, String id, Map<String, Object>filedValueMap) {
	  
	    UpdateRequest ur = null;
		try {
			
			XContentBuilder builder = jsonBuilder().startObject();
			for(String field : filedValueMap.keySet()){
				builder.field(field, filedValueMap.get(field));
			}
			builder.endObject();
			ur = new UpdateRequest(index, type, id).doc(builder);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	    return ur;
  }

  public static void main(String[] args) {
    ESDriver es = new ESDriver("cody");
    es.getTypeListWithPrefix("podaacsession", "sessionstats");
  }

}
