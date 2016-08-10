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
package esiptestbed.mudrod.weblog.structure;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.joda.time.Seconds;
import org.elasticsearch.common.joda.time.format.DateTimeFormatter;
import org.elasticsearch.common.joda.time.format.ISODateTimeFormat;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import esiptestbed.mudrod.discoveryengine.MudrodAbstract;
import esiptestbed.mudrod.driver.ESDriver;

public class Session extends MudrodAbstract implements Comparable<Session> {
  private String start;
  private String end;
  private String id;
  private String newid = null;
  private DateTimeFormatter fmt = ISODateTimeFormat.dateTime();
  private String type;// es type

  public Session(Map<String, String> config, ESDriver es, String Start,
      String end, String id) {
    super(config, es, null);
    this.start = start;
    this.end = end;
    this.id = id;
  }

  public Session(Map<String, String> config, ESDriver es) {
    super(config, es, null);
  }

  public String getID() {
    return id;
  }

  public String getNewID() {
    return newid;
  }

  public String setNewID(String str) {
    return newid = str;
  }

  public String getStartTime() {
    return start;
  }

  public String getEndTime() {
    return end;
  }

  public int compareTo(Session o) {
    fmt.parseDateTime(this.end);
    fmt.parseDateTime(o.end);
    // ascending order
    int result = Seconds
        .secondsBetween(fmt.parseDateTime(o.end), fmt.parseDateTime(this.end))
        .getSeconds();
    return result;
  }

  // used for session tree reconstruct
  public JsonObject getSessionDetail(String cleanuptype, String SessionID)
      throws UnsupportedEncodingException {
    JsonObject SessionResults = new JsonObject();
    Gson gson = new Gson();
    // for session tree
    SessionTree tree = this.getSessionTree(cleanuptype, SessionID);
    JsonObject jsonTree = tree.TreeToJson(tree.root);
    SessionResults.add("treeData", jsonTree);
    // for request
    JsonElement jsonRequest = this.getRequests(cleanuptype, SessionID);
    SessionResults.add("RequestList", jsonRequest);

    return SessionResults;
  }

  public List<ClickStream> getClickStreamList(String cleanuptype,
      String SessionID) throws UnsupportedEncodingException {
    SessionTree tree = this.getSessionTree(cleanuptype, SessionID);
    List<ClickStream> clickthroughs = tree.getClickStreamList();
    return clickthroughs;
  }

  private SessionTree getSessionTree(String cleanuptype, String SessionID)
      throws UnsupportedEncodingException {
    SearchResponse response = es.client.prepareSearch(config.get("indexName"))
        .setTypes(cleanuptype)
        .setQuery(QueryBuilders.termQuery("SessionID", SessionID)).setSize(100)
        .addSort("Time", SortOrder.ASC).execute().actionGet();
    int size = response.getHits().getHits().length;

    Gson gson = new Gson();
    SessionTree tree = new SessionTree(this.config, this.es, SessionID,
        cleanuptype);
    int seq = 1;
    for (SearchHit hit : response.getHits().getHits()) {
      Map<String, Object> result = hit.getSource();
      String request = (String) result.get("Request");
      String requestUrl = (String) result.get("RequestUrl");
      String time = (String) result.get("Time");
      String logType = (String) result.get("LogType");
      String referer = (String) result.get("Referer");

      SessionNode node = new SessionNode(request, logType, referer, time, seq);
      tree.insert(node);
      seq++;
    }

    return tree;
  }

  private JsonElement getRequests(String cleanuptype, String SessionID)
      throws UnsupportedEncodingException {
    SearchResponse response = es.client.prepareSearch(config.get("indexName"))
        .setTypes(cleanuptype)
        .setQuery(QueryBuilders.termQuery("SessionID", SessionID)).setSize(100)
        .addSort("Time", SortOrder.ASC).execute().actionGet();
    int size = response.getHits().getHits().length;

    Gson gson = new Gson();
    List<JsonObject> requestList = new ArrayList<JsonObject>();
    int seq = 1;
    for (SearchHit hit : response.getHits().getHits()) {
      Map<String, Object> result = hit.getSource();
      String request = (String) result.get("Request");
      String requestUrl = (String) result.get("RequestUrl");
      String time = (String) result.get("Time");
      String logType = (String) result.get("LogType");
      String referer = (String) result.get("Referer");

      JsonObject req = new JsonObject();
      req.addProperty("Time", time);
      req.addProperty("Request", request);
      req.addProperty("RequestURL", requestUrl);
      req.addProperty("LogType", logType);
      req.addProperty("Referer", referer);
      req.addProperty("Seq", seq);
      requestList.add(req);

      seq++;
    }

    JsonElement jsonElement = gson.toJsonTree(requestList);
    return jsonElement;
  }
}
