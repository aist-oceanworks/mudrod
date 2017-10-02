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
package gov.nasa.jpl.mudrod.weblog.structure;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.Serializable;

/**
 * ClassName: ClickStream Function: user click stream data related operations.
 */
public class ClickStream implements Serializable {
  /**
   *
   */
  private static final long serialVersionUID = 1L;
  // keywords: query words related to the click behaviour
  private String keywords;
  // viewDataset: the dataset name user viewed
  private String viewDataset;
  // downloadDataset: the dataset name user downloaded
  private String downloadDataset;
  // sessionID: session ID
  private String sessionID;
  // type: session type name
  private String type;

  /**
   * Creates a new instance of ClickStream.
   *
   * @param keywords    the query user searched
   * @param viewDataset the dataset name user viewed
   * @param download:   if user download the data set after viewing it, this parameter is
   *                    true, otherwise, it is false.
   */
  public ClickStream(String keywords, String viewDataset, boolean download) {
    this.keywords = keywords;
    this.viewDataset = viewDataset;
    this.downloadDataset = "";
    if (download) {
      this.downloadDataset = viewDataset;
    }
  }

  public ClickStream() {
    //default constructor
  }

  public String getSessionID() {
    return sessionID;
  }

  /**
   * setKeyWords: Set the query user searched.
   *
   * @param query search words
   */
  public void setKeyWords(String query) {
    this.keywords = query;
  }

  /**
   * setViewDataset:Set the data set name user viewed
   *
   * @param dataset short name of data set
   */
  public void setViewDataset(String dataset) {
    this.viewDataset = dataset;
  }

  /**
   * setDownloadDataset: Set the data set name user downloaded
   *
   * @param dataset short name of data set
   */
  public void setDownloadDataset(String dataset) {
    this.downloadDataset = dataset;
  }

  /**
   * getKeyWords: Get the query user searched
   *
   * @return data set name
   */
  public String getKeyWords() {
    return this.keywords;
  }

  /**
   * getViewDataset: Get the data set user viewed
   *
   * @return data set name
   */
  public String getViewDataset() {
    return this.viewDataset;
  }

  /**
   * isDownload: Show whether the data is downloaded in the session.
   *
   * @return True or False
   */
  public Boolean isDownload() {
    if ("".equals(this.downloadDataset)) {
      return false;
    }
    return true;
  }

  /**
   * setSessionId: Set ID of session
   *
   * @param sessionID session id
   */
  public void setSessionId(String sessionID) {
    this.sessionID = sessionID;
  }

  /**
   * setType: Set session type name
   *
   * @param type session type name in elasticsearch
   */
  public void setType(String type) {
    this.type = type;
  }

  /**
   * Output click stream info in string format
   *
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "Query: " + keywords + " || View Dataset: " + viewDataset + " || Download Dataset: " + downloadDataset;
  }

  /**
   * toJson: Output click stream info in Json format
   *
   * @return session in string format
   */
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

  /**
   * parseFromTextLine: Convert string to click stream data
   *
   * @param logline http log line
   * @return {@link ClickStream}
   */
  public static ClickStream parseFromTextLine(String logline) {
    JSONObject jsonData = null;
    ClickStream data = null;
    try {
      jsonData = new JSONObject(logline);
      data = new ClickStream();
      data.setKeyWords(jsonData.getString("query"));
      data.setViewDataset(jsonData.getString("viewdataset"));
      data.setDownloadDataset(jsonData.getString("downloaddataset"));

    } catch (JSONException e) {
      e.printStackTrace();
    }

    return data;
  }
}
