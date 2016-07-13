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

import java.io.Serializable;

import org.codehaus.jettison.json.JSONObject;

public class ClickStream implements Serializable {
  private String keywords;
  private String viewDataset;
  private String downloadDataset;
  private String sessionID;
  private String type;

  public ClickStream(String keywords, String viewDataset, boolean download) {
    this.keywords = keywords;
    this.viewDataset = viewDataset;
    this.downloadDataset = "";
    if (download) {
      this.downloadDataset = viewDataset;
    }
  }

  public ClickStream() {

  }

  public void setKeyWords(String query) {
    this.keywords = query;
  }

  public void setViewDataset(String dataset) {
    this.viewDataset = dataset;
  }

  public void setDownloadDataset(String dataset) {
    this.downloadDataset = dataset;
  }

  public String getKeyWords() {
    return this.keywords;
  }

  public String getViewDataset() {
    return this.viewDataset;
  }

  public Boolean isDownload() {
    if (this.downloadDataset.equals("")) {
      return false;
    }
    return true;
  }

  public void setSessionId(String sessionID) {
    this.sessionID = sessionID;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String toString() {
    return "query:" + keywords + "|| view dataset:" + viewDataset
        + "|| download Dataset:" + downloadDataset;
  }

  public String toJson() {
    String jsonQuery = "{";
    jsonQuery += "\"query\":\"" + this.keywords + "\",";
    jsonQuery += "\"viewdataset\":\"" + this.viewDataset + "\",";
    jsonQuery += "\"downloaddataset\":\"" + this.downloadDataset + "\",";
    jsonQuery += "\"sessionId\":\"" + this.sessionID + "\",";
    jsonQuery += "\"type\":\"" + this.type + "\"";
    jsonQuery += "},";
    return jsonQuery;
  }

  public static ClickStream parseFromTextLine(String logline) throws Exception {
    JSONObject jsonData = new JSONObject(logline);
    ClickStream data = new ClickStream();
    data.setKeyWords(jsonData.getString("query"));
    data.setViewDataset(jsonData.getString("viewdataset"));
    data.setDownloadDataset(jsonData.getString("downloaddataset"));

    return data;
  }

}
