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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
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

/**
 * Supports Elasticsearch related functions
 *
 */
public class ESDriver implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(ESDriver.class);
  private static final long serialVersionUID = 1L;
  public Client client = null;
  public Node node = null;
  public BulkProcessor bulkProcessor = null;

  /**
   * Start an Elasticsearch client by cluster name
   * @param clusterName name of Elasticsearch cluster
   */
  public ESDriver(String clusterName){
    node =
        nodeBuilder()
        .settings(ImmutableSettings.settingsBuilder().put("http.enabled", false))
        .clusterName(clusterName)
        .client(true)
        .node();

    client = node.client();
  }

  /**
   * Start an Elasticsearch client by cluster name
   * @param config MUDROD config map
   */
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

  /**
   * Method of creating a bulk processor
   */
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

  /**
   * Method of closing a bulk processor
   */
  public void destroyBulkProcessor(){
    try {
      bulkProcessor.awaitClose(20, TimeUnit.MINUTES);
      bulkProcessor = null;
      refreshIndex();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  /**
   * Method of setting Elasticsearch schema by setting and mapping JSON object
   * @param indexName name of Elasticsearch index
   * @param settings_json setting in JSON format
   * @param mapping_json mapping in JSON format
   * @throws IOException
   */
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

  /**
   * Method of analyzing a string through customized analyzer
   * @param indexName name of index
   * @param str String needed to be processed
   * @return Processed string
   * @throws InterruptedException
   * @throws ExecutionException
   */
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

  /**
   * Method of analyzing a string through customized analyzer
   * @param indexName The name of index
   * @param list The list of array that needs to be processed
   * @return A processed list of String
   * @throws InterruptedException
   * @throws ExecutionException
   */
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

  /**
   * Method of deleting documents from Elasticsearch by a particular query
   * @param index The name of index
   * @param type The name of type
   * @param query a particular query used for delete
   */
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

  /**
   * Method of deleting a type from a index
   * @param index The name of index
   * @param type The name of type
   */
  public void deleteType(String index, String type){
    this.deleteAllByQuery(index, type, QueryBuilders.matchAllQuery());
  }

  /**
   * Method of getting a list of types given a prefix
   * @param index The name of index
   * @param typePrefix Prefix string
   * @return
   */
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
  
  /**
   * Method of checking if a type exists in an index or not
   * @param index The name of index
   * @param type The name of type
   * @return 1 if it exists, 0 otherwise
   */
  public boolean checkTypeExist(String index, String type)
  {
    GetMappingsResponse res;
    try {
      res = client.admin().indices().getMappings(new GetMappingsRequest().indices(index)).get();
      ImmutableOpenMap<String, MappingMetaData> mapping  = res.mappings().get(index);
      for (ObjectObjectCursor<String, MappingMetaData> c : mapping) {
        if(c.key.equals(type))
        {
          return true;
        }
      }
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (ExecutionException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return false;     
  }

  /**
   * Get a list of terms give a string
   * @param index The name of index
   * @param chars The piece of string
   * @return A list of string for autocompletion
   */
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

  /**
   * Method of closing Elasticsearch client
   */
  public void close(){
    node.close();
  }

  /**
   * Method of refreshing Elasticsearch storage
   */
  public void refreshIndex(){
    node.client().admin().indices().prepareRefresh().execute().actionGet();
  }

}
