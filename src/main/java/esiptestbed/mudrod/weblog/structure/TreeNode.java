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

public class TreeNode {
  public String id;
  public List<TreeNode> children = new ArrayList<TreeNode>();
  public TreeNode parent; 

  public String time;
  public String request;
  public String referer;
  public int seq;

  public String key; // dataset/datasetlist/ftp
  public String logType; //po.dacc/ftp
  public String search;
  public Map<String, String> filter;
  public String datasetId;

  //should be delete
  public String value;

  public TreeNode(){

  }

  public TreeNode(String request, String logType, String referer, String time, int seq ){
    this.logType = logType;
    this.time = time;
    this.seq = seq;
    this.setRequest(request);
    this.setReferer(referer);
    this.setKey(request, logType);
  }

  public void setReferer(String referer) {
    if(referer == null){
      this.referer = "";
      return;
    }

    this.referer = referer.toLowerCase().replace("http://podaac.jpl.nasa.gov", "");


    //"http://podaac.jpl.nasa.gov"
  }

  public void setRequest(String req) {
    this.request = req;
    if (this.logType.equals("PO.DAAC")){
      this.parseRequest(req);
    }
  }

  public void parseRequest(String request) {
    Pattern pattern = Pattern.compile("get (.*?) http/*");
    Matcher matcher = pattern.matcher(request.trim().toLowerCase());
    while (matcher.find()) {
      request = matcher.group(1);
    }

    /*Map<String, String> mapRequest = RequestUrl.URLRequest(request);
		String search = mapRequest.get("search");
		if (search != null) {
			search = URLDecoder.decode(mapRequest.get("search"));
		} else {
			search = "";
		}

		this.search = search;
		this.parseFilterParams(mapRequest);*/

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

    if(!this.search.equals("")){
      this.filter.put("search", this.search);
    }
  }

  public List<TreeNode> getChildren() {
    return this.children;
  }

  public void setChildren(List<TreeNode> children) {
    this.children = children;
  }

  public void addChildren(TreeNode node) {
    this.children.add(node);
  }

  public String getId() {
    return this.id;
  }

  public String toString() {
    return /*this.id + " " + this.parentId + " " + */this.time + " " + this.request + " " + this.logType /* + " " + this.search + " "
				+ this.getFilterStr() + " " + this.datasetId*/;
  }

  public Boolean bSame(TreeNode node) {
    Boolean bsame = false;
    if (this.request.equals(node.request)) {
      bsame = true;
    }
    return bsame;
  }

  public void setKey(String request, String logType){
    this.key = "";
    String datasetlist = "/datasetlist?";
    String dataset = "/dataset/";
    //String granule = "/granule/";
    if (logType.equals("ftp")) {
      this.key = "ftp";
    } else if (logType.equals("root")) {
      this.key = "root";
    } else {
      if (request.contains(datasetlist)) {
        this.key = "datasetlist";
      } else if (request.contains(dataset) /*|| request.contains(granule)*/) {
        this.key = "dataset";
      }
    }
  }

  public void parseDatasetId(String request){
    try {
      request = URLDecoder.decode(request,"UTF-8");
    } catch (UnsupportedEncodingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    String[] twoparts = request.split("[?]");
    String[] parts = twoparts[0].split("/");
    if(parts.length <=2){
      System.out.println(request);
      return;
    }
    this.datasetId = parts[2];
  }

  public String getKey(){
    return this.key;
  }

  public String getRequest(){
    return this.request;
  }

  public String getReferer(){
    return this.referer;
  }

  public TreeNode getParent(){
    return this.parent;
  }

  public void setParent(TreeNode parent){
    this.parent = parent;
  }

  public String getSearch(){
    return  this.search;
  }

  public Map<String, String> getFilter(){
    return this.filter;
  }

  public String getFilterStr(){
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

  public String getDatasetId(){
    return this.datasetId;
  }

  public int getSeq(){
    return this.seq;
  }
}
