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

public class SessionNode {
  protected String id;
  protected String value;
  protected SessionNode parent;
  protected List<SessionNode> children = new ArrayList<SessionNode>();
  protected String time;
  protected String request;
  protected String referer;
  protected int seq;
  protected String key; // dataset/datasetlist/ftp
  protected String logType; // po.dacc/ftp
  protected String search;
  protected Map<String, String> filter;
  protected String datasetId;

  public SessionNode() {

  }

  public SessionNode(String request, String logType, String referer,
      String time, int seq) {
    this.logType = logType;
    this.time = time;
    this.seq = seq;
    this.setRequest(request);
    this.setReferer(referer);
    this.setKey(request, logType);
  }

  public void setReferer(String referer) {
    if (referer == null) {
      this.referer = "";
      return;
    }
    this.referer = referer.toLowerCase().replace("http://podaac.jpl.nasa.gov",
        "");
  }

  public void setRequest(String req) {
    this.request = req;
    if (this.logType.equals("PO.DAAC")) {
      this.parseRequest(req);
    }
  }

  public List<SessionNode> getChildren() {
    return this.children;
  }

  public void setChildren(List<SessionNode> children) {
    this.children = children;
  }

  public void addChildren(SessionNode node) {
    this.children.add(node);
  }

  public String getId() {
    return this.id;
  }

  public Boolean bSame(SessionNode node) {
    Boolean bsame = false;
    if (this.request.equals(node.request)) {
      bsame = true;
    }
    return bsame;
  }

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
      } else if (request.contains(dataset) /* || request.contains(granule) */) {
        this.key = "dataset";
      }
    }
  }

  public String getKey() {
    return this.key;
  }

  public String getRequest() {
    return this.request;
  }

  public String getReferer() {
    return this.referer;
  }

  public SessionNode getParent() {
    return this.parent;
  }

  public void setParent(SessionNode parent) {
    this.parent = parent;
  }

  public String getSearch() {
    return this.search;
  }

  public Map<String, String> getFilter() {
    return this.filter;
  }

  public String getDatasetId() {
    return this.datasetId;
  }

  public int getSeq() {
    return this.seq;
  }

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

  public void parseDatasetId(String request) {
    try {
      request = URLDecoder.decode(request, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    String[] twoparts = request.split("[?]");
    String[] parts = twoparts[0].split("/");
    if (parts.length <= 2) {
      // System.out.println(request);
      return;
    }
    this.datasetId = parts[2];
  }
}
