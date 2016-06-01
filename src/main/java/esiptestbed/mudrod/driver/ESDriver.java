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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse.AnalyzeToken;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;


public class ESDriver {
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
            System.out.println("Bulk Fail!");
            throw new RuntimeException("Caught exception in bulk: " + request + ", failure: " + failure, failure);
          } 
        }
        )
        .setBulkActions(1000) 
        .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB)) 
        .setFlushInterval(TimeValue.timeValueSeconds(5))    //let's test this
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
    String[] str_list = str.split(",");
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

  public void close(){
    node.close();   
  }

  public void refreshIndex(){
    node.client().admin().indices().prepareRefresh().execute().actionGet();
  }

  public static void main(String[] args) {
    // TODO Auto-generated method stub

  }

}
