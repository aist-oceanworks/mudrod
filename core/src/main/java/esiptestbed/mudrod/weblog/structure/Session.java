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
import java.util.Properties;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.joda.time.Seconds;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import esiptestbed.mudrod.driver.ESDriver;

/**
 * ClassName: Session Function: Session operations.
 *
 */
public class Session /*extends MudrodAbstract*/ implements Comparable<Session> {
  private static final Logger LOG = LoggerFactory.getLogger(Session.class);
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  // start: start time of session
  private String start;
  // end: end time of session
  private String end;
  // id: original session ID
  private String id;
  // newid: new session ID
  private String newid = null;
  // fmt: time formatter
  private DateTimeFormatter fmt = ISODateTimeFormat.dateTime();
  // type: session type name in Elasticsearch
  private String type;

  private ESDriver es;
  private Properties props;

  /**
   * Creates a new instance of Session.
   *
   * @param props
   *          the Mudrod configuration
   * @param es
   *          the Elasticsearch drive
   * @param start
   *          start time of session
   * @param end
   *          end time of session
   * @param id
   *          session ID
   */
  public Session(Properties props, ESDriver es, String start, String end,
      String id) {
    // super(props, es, null);
    this.start = start;
    this.end = end;
    this.id = id;

    this.props = props;
    this.es = es;
  }

  /**
   * Creates a new instance of Session.
   *
   * @param props
   *          the Mudrod configuration
   * @param es
   *          the Elasticsearch drive
   */
  public Session(Properties props, ESDriver es) {
    // super(props, es, null);
    this.props = props;
    this.es = es;
  }

  /**
   * getID: Get original session ID
   *
   * @return session id
   */
  public String getID() {
    return id;
  }

  /**
   * getNewID: Get new session ID
   *
   * @return new session id
   */
  public String getNewID() {
    return newid;
  }

  /**
   * setNewID: Set new session ID
   *
   * @param str:
   *          session ID
   * @return new session id
   */
  public String setNewID(String str) {
    return newid = str;
  }

  /**
   * getStartTime:Get start time of current session
   *
   * @return start time of session
   */
  public String getStartTime() {
    return start;
  }

  /**
   * getEndTime:Get end time of current session
   *
   * @return end time of session
   */
  public String getEndTime() {
    return end;
  }

  /**
   * Compare current session with another session
   *
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  @Override
  public int compareTo(Session o) {
    fmt.parseDateTime(this.end);
    fmt.parseDateTime(o.end);
    // ascending order
    return Seconds
        .secondsBetween(fmt.parseDateTime(o.end), fmt.parseDateTime(this.end))
        .getSeconds();

  }

  /**
   * getSessionDetail:Get detail of current session, which is used for session
   * tree reconstruct
   *
   * @param indexName name of index from which you wish to obtain session detail.
   * @param cleanuptype:
   *          Session type name in Elasticsearch
   * @param sessionID:
   *          Session ID
   * @return Session details in Json format
   */
  public JsonObject getSessionDetail(String indexName, String cleanuptype,
      String sessionID) {
    JsonObject sessionResults = new JsonObject();
    // for session tree
    SessionTree tree = null;
    JsonElement jsonRequest = null;
    try {
      tree = this.getSessionTree(indexName, cleanuptype, sessionID);
      JsonObject jsonTree = tree.TreeToJson(tree.root);
      sessionResults.add("treeData", jsonTree);

      jsonRequest = this.getRequests(cleanuptype, sessionID);
      sessionResults.add("RequestList", jsonRequest);
    } catch (UnsupportedEncodingException e) {
      LOG.error("Encoding error detected.", e);

    }

    return sessionResults;
  }

  /**
   * getClickStreamList: Extracted click stream list from current session.
   *
   * @param cleanuptype:
   *          Session type name in Elasticsearch
   * @param sessionID:
   *          Session ID
   * @return Click stram data list
   *         {@link esiptestbed.mudrod.weblog.structure.ClickStream}
   */
  public List<ClickStream> getClickStreamList(String indexName,
      String cleanuptype, String sessionID) {
    SessionTree tree = null;
    try {
      tree = this.getSessionTree(indexName, cleanuptype, sessionID);
    } catch (UnsupportedEncodingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    List<ClickStream> clickthroughs = tree.getClickStreamList();
    return clickthroughs;
  }

  /**
   * Method of converting a given session to a tree structure
   * 
   * @param cleanuptype
   *          session type name in Elasticsearch
   * @param sessionID
   *          ID of session
   * @return an instance of session tree structure
   * @throws UnsupportedEncodingException
   *           UnsupportedEncodingException
   */
  private SessionTree getSessionTree(String indexName, String cleanuptype,
      String sessionID) throws UnsupportedEncodingException {

    SearchResponse response = es.getClient().prepareSearch(indexName)
        .setTypes(cleanuptype)
        .setQuery(QueryBuilders.termQuery("SessionID", sessionID)).setSize(100)
        .addSort("Time", SortOrder.ASC).execute().actionGet();
    int size = response.getHits().getHits().length;

    Gson gson = new Gson();
    SessionTree tree = new SessionTree(this.props, this.es, sessionID,
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

  /**
   * Method of getting all requests from a given current session
   * 
   * @param cleanuptype
   *          Session type name in Elasticsearch
   * @param sessionID
   *          Session ID
   * @return all of these requests in JSON
   * @throws UnsupportedEncodingException
   *           UnsupportedEncodingException
   */
  private JsonElement getRequests(String cleanuptype, String sessionID)
      throws UnsupportedEncodingException {
    SearchResponse response = es.getClient()
        .prepareSearch(props.getProperty("indexName")).setTypes(cleanuptype)
        .setQuery(QueryBuilders.termQuery("SessionID", sessionID)).setSize(100)
        .addSort("Time", SortOrder.ASC).execute().actionGet();
    int size = response.getHits().getHits().length;

    Gson gson = new Gson();
    List<JsonObject> requestList = new ArrayList<>();
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

  /**
   * getClickStreamList: Extracted ranking training data from current session.
   *
   * @param cleanuptype:
   *          Session type name in Elasticsearch
   * @param sessionID:
   *          Session ID
   * @return Click stram data list
   *         {@link esiptestbed.mudrod.weblog.structure.ClickStream}
   */
  public List<RankingTrainData> getRankingTrainData(String indexName,
      String cleanuptype, String sessionID) {
    SessionTree tree = null;
    try {
      tree = this.getSessionTree(indexName, cleanuptype, sessionID);
    } catch (UnsupportedEncodingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    List<RankingTrainData> trainData = null;
    try {
      trainData = tree.getRankingTrainData(indexName, cleanuptype, sessionID);
    } catch (UnsupportedEncodingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return trainData;
  }
}
