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

/**
 * ClassName: Session <br/>
 * Function: Session operations. <br/>
 * Date: Aug 12, 2016 1:04:31 PM <br/>
 *
 * @author Yun
 * @version 
 */
public class Session extends MudrodAbstract implements Comparable<Session> {
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

	/**
	 * Creates a new instance of Session.
	 *
	 * @param config the Mudrod configuration
	 * @param es the Elasticsearch drive
	 * @param Start start time of session
	 * @param end  end time of session
	 * @param id session ID
	 */
	public Session(Map<String, String> config, ESDriver es, String Start, String end, String id) {
		super(config, es, null);
		this.start = start;
		this.end = end;
		this.id = id;
	}

	/**
	 * Creates a new instance of Session.
	 *
	 * @param config the Mudrod configuration
	 * @param es the Elasticsearch drive
	 */
	public Session(Map<String, String> config, ESDriver es) {
		super(config, es, null);
	}

	/**
	 * getID: Get original session ID
	 */
	public String getID() {
		return id;
	}

	/**
	 * getNewID: Get new session ID
	 * @return
	 */
	public String getNewID() {
		return newid;
	}

	/**
	 * setNewID: Set new session ID
	 * @param str: session ID
	 * @return
	 */
	public String setNewID(String str) {
		return newid = str;
	}

	/**
	 * getStartTime:Get start time of current session
	 * @return
	 */
	public String getStartTime() {
		return start;
	}

	/**
	 * getEndTime:Get end time of current session
	 * @return
	 */
	public String getEndTime() {
		return end;
	}

	/**
	 * Compare current session with another session
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	public int compareTo(Session o) {
		// TODO Auto-generated method stub
		// String compareEnd = o.end;
		fmt.parseDateTime(this.end);
		fmt.parseDateTime(o.end);
		// ascending order
		int result = Seconds.secondsBetween(fmt.parseDateTime(o.end), fmt.parseDateTime(this.end)).getSeconds();
		return result;
	}

	/**
	 * getSessionDetail:Get detail of current session, which is used for session tree reconstruct
	 * @param cleanuptype: Session type name in Elasticsearch
	 * @param SessionID: Session ID
	 * @return Session details in Json format
	 */
	public JsonObject getSessionDetail(String cleanuptype, String SessionID) throws UnsupportedEncodingException {
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

	/**
	 * getClickStreamList: Extracted click stream list from current session. <br/>
	 * @param cleanuptype: Session type name in Elasticsearch
	 * @param SessionID: Session ID
	 * @return Click stram data list {@link esiptestbed.mudrod.weblog.structure.ClickStream}
	 */
	public List<ClickStream> getClickStreamList(String cleanuptype, String SessionID)
			throws UnsupportedEncodingException {
		SessionTree tree = this.getSessionTree(cleanuptype, SessionID);
		List<ClickStream> clickthroughs = tree.getClickStreamList();
		return clickthroughs;
	}

	/**
	 * getSessionTree: Convert current session to a tree structure
	 * @param cleanuptype: Session type name in Elasticsearch
	 * @param SessionID: Session ID
	 * @return Session Tree {@link esiptestbed.mudrod.weblog.structure.SessionTree}
	 */
	private SessionTree getSessionTree(String cleanuptype, String SessionID) throws UnsupportedEncodingException {
		SearchResponse response = es.client.prepareSearch(config.get("indexName")).setTypes(cleanuptype)
				.setQuery(QueryBuilders.termQuery("SessionID", SessionID)).setSize(100).addSort("Time", SortOrder.ASC)
				.execute().actionGet();
		int size = response.getHits().getHits().length;

		Gson gson = new Gson();
		SessionTree tree = new SessionTree(this.config, this.es, SessionID, cleanuptype);
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
	 * getRequests: Get all request in current session
	 * @param cleanuptype: Session type name in Elasticsearch
	 * @param SessionID: Session ID
	 * @return all requests in Json format
	 */
	private JsonElement getRequests(String cleanuptype, String SessionID) throws UnsupportedEncodingException {
		SearchResponse response = es.client.prepareSearch(config.get("indexName")).setTypes(cleanuptype)
				.setQuery(QueryBuilders.termQuery("SessionID", SessionID)).setSize(100).addSort("Time", SortOrder.ASC)
				.execute().actionGet();
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
