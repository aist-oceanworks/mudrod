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
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ClassName: SessionNode <br/>
 * Function: Functions related to a node in a session tree sturcture<br/>
 * Reason: TODO ADD REASON(可选). <br/>
 * Date: Aug 12, 2016 1:16:41 PM <br/>
 *
 * @author Yun
 * @version 
 */
public class SessionNode {
	// id: Node ID
	protected String id;
	// value: Node value 
	protected String value;
	// parent: Parent node of this node
	protected SessionNode parent;
	// children: Child nodes of this node
	protected List<SessionNode> children = new ArrayList<>();
	// time: request time of node
	protected String time;
	// request: request url of this node
	protected String request;
	// referer: previous visiting url of this node
	protected String referer;
	// seq: sequence of this node
	protected int seq;
	// key: type of this node extracted from url, including three types - dataset,datasetlist,ftp
	protected String key;
	// logType: log types of this node, including two types - po.dacc, ftp
	protected String logType; 
	// search: query extracted from this node
	protected String search;
	// filter:  filter facets extracted from this node
	protected Map<String, String> filter;
	// datasetId: viewed/downloaded data set ID 
	protected String datasetId;

	public SessionNode() {

	}

	/**
	 * Creates a new instance of SessionNode.
	 *
	 * @param request: request url
	 * @param logType: including two types - po.dacc, ftp
	 * @param referer: previous visiting url
	 * @param time: request time of node
	 * @param seq: sequence of this node
	 */
	public SessionNode(String request, String logType, String referer, String time, int seq) {
		this.logType = logType;
		this.time = time;
		this.seq = seq;
		this.setRequest(request);
		this.setReferer(referer);
		this.setKey(request, logType);
	}

	/**
	 * setReferer: Set previous visiting url of this node
	 * @param referer
	 */
	public void setReferer(String referer) {
		if (referer == null) {
			this.referer = "";
			return;
		}
		this.referer = referer.toLowerCase().replace("http://podaac.jpl.nasa.gov", "");
	}

	/**
	 * setRequest: Set visiting url of this node
	 * @param req
	 */
	public void setRequest(String req) {
		this.request = req;
		if (this.logType.equals("PO.DAAC")) {
			this.parseRequest(req);
		}
	}

	/**
	 * getChildren:Get child nodes of this node
	 * @return child nodes
	 */
	public List<SessionNode> getChildren() {
		return this.children;
	}

	/**
	 * setChildren: Set child nodes of this node
	 * @param children
	 */
	public void setChildren(List<SessionNode> children) {
		this.children = children;
	}

	/**
	 * addChildren: Add a children node 
	 * @param node
	 */
	public void addChildren(SessionNode node) {
		this.children.add(node);
	}

	/**
	 * getId:Get node ID
	 * @return
	 */
	public String getId() {
		return this.id;
	}

	/**
	 * bSame:Compare this node with another node
	 * @param node {@link esiptestbed.mudrod.weblog.structure.SessionNode}
	 * @return
	 */
	public Boolean bSame(SessionNode node) {
		Boolean bsame = false;
		if (this.request.equals(node.request)) {
			bsame = true;
		}
		return bsame;
	}

	/**
	 * setKey:Set request type which contains three categories - dataset,datasetlist,ftp
	 * @param request
	 * @param logType
	 */
	public void setKey(String request, String logType) {
		this.key = "";
		String datasetlist = "/datasetlist?";
		String dataset = "/dataset/";
		if (logType.equals("ftp")) {
			this.key = "ftp";
		} else if (logType.equals("root")) {
			this.key = "root";
		} else {
			if (request.contains(datasetlist)) {
				this.key = "datasetlist";
			} else if (request
					.contains(dataset) /* || request.contains(granule) */) {
				this.key = "dataset";
			}
		}
	}

	/**
	 * getKey:Get request type which contains three categories - dataset,datasetlist,ftp
	 * @return
	 */
	public String getKey() {
		return this.key;
	}

	/**
	 * getRequest:Get node request
	 * @return
	 */
	public String getRequest() {
		return this.request;
	}

	/**
	 * getReferer:Get previous visiting url of this node
	 * @return
	 */
	public String getReferer() {
		return this.referer;
	}

	/**
	 * getParent:Get parent node of this node
	 * @return
	 */
	public SessionNode getParent() {
		return this.parent;
	}

	/**
	 * setParent: Set parent node of this node
	 * @param parent
	 */
	public void setParent(SessionNode parent) {
		this.parent = parent;
	}

	/**
	 * getSearch:Get query of this node
	 * @return
	 */
	public String getSearch() {
		return this.search;
	}

	/**
	 * getFilter:Get filter facets of this node
	 * @return
	 */
	public Map<String, String> getFilter() {
		return this.filter;
	}

	/**
	 * getDatasetId:Get data set ID of this node
	 * @return
	 */
	public String getDatasetId() {
		return this.datasetId;
	}

	/**
	 * getSeq:Get sequence of this node
	 * @return
	 */
	public int getSeq() {
		return this.seq;
	}

	/**
	 * getFilterStr:Get filter facets of this node
	 * @return
	 */
	public String getFilterStr() {
		String filter = "";
		if (this.filter.size() > 0) {
			Iterator iter = this.filter.keySet().iterator();
			while (iter.hasNext()) {
				String key = (String) iter.next();
				String val = this.filter.get(key);
				filter += key + "=" + val + ",";
			}

			filter = filter.substring(0, filter.length() - 1);
		}

		return filter;
	}

	/**
	 * parseRequest:Parse request to extract request type
	 * @param request
	 */
	public void parseRequest(String request) {
		Pattern pattern = Pattern.compile("get (.*?) http/*");
		Matcher matcher = pattern.matcher(request.trim().toLowerCase());
		while (matcher.find()) {
			request = matcher.group(1);
		}
		if (request.contains("/dataset/")) {
			this.parseDatasetId(request);
		}

		this.request = request.toLowerCase();
	}

	/**
	 * parseFilterParams:Parse filter facets information
	 * @param params
	 */
	private void parseFilterParams(Map<String, String> params) {
		this.filter = new HashMap<String, String>();
		if (params.containsKey("ids")) {
			String idsStr = params.get("ids");
			if (!idsStr.equals("")) {
				idsStr = URLDecoder.decode(idsStr);
				String[] ids = idsStr.split(":");
				String valueStr = params.get("values");
				if (valueStr != null) {
					valueStr = URLDecoder.decode(valueStr);
					String[] values = valueStr.split(":");
					int size = ids.length;
					for (int i = 0; i < size; i++) {
						this.filter.put(ids[i], values[i]);
					}
				}
			}
		}

		if (!this.search.equals("")) {
			this.filter.put("search", this.search);
		}
	}

	/**
	 * parseDatasetId:Parse Request to extract data set ID
	 * @param request
	 */
	public void parseDatasetId(String request) {
		try {
			request = URLDecoder.decode(request, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		String[] twoparts = request.split("[?]");
		String[] parts = twoparts[0].split("/");
		if (parts.length <= 2) {
			return;
		}
		this.datasetId = parts[2];
	}
}
