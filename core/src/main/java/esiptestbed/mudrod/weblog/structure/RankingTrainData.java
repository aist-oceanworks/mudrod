package esiptestbed.mudrod.weblog.structure;

import java.io.Serializable;
import java.util.Map;

/**
 * ClassName: train data extracted from web logs for training ranking weightss.
 */
public class RankingTrainData implements Serializable {
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  // sessionID: session ID
  private String sessionID;
  // type: session type name
  private String index;
  // query: query words related to the click
  private String query;
  // datasetA
  private String highRankDataset;
  // datasetB
  private String lowRankDataset;

  private Map<String, String> filter;

  /**
   * Creates a new instance of ClickStream.
   *
   * @param keywords
   *          the query user searched
   * @param viewDataset
   *          the dataset name user viewed
   * @param download:
   *          if user download the data set after viewing it, this parameter is
   *          true, otherwise, it is false.
   */
  public RankingTrainData(String query, String highRankDataset,
      String lowRankDataset) {
    this.query = query;
    this.highRankDataset = highRankDataset;
    this.lowRankDataset = lowRankDataset;
  }

  public RankingTrainData() {

  }

  public String getSessionID() {
    return sessionID;
  }

  /**
   * setKeyWords: Set the query user searched.
   *
   * @param query
   *          search words
   */
  public void setQuery(String query) {
    this.query = query;
  }

  /**
   * getKeyWords: Get the query user searched
   *
   * @return data set name
   */
  public String getQuery() {
    return this.query;
  }

  /**
   * setViewDataset:Set the data set name user viewed
   *
   * @param dataset
   *          short name of data set
   */
  public void setHighRankDataset(String dataset) {
    this.highRankDataset = dataset;
  }

  /**
   * setDownloadDataset: Set the data set name user downloaded
   *
   * @param dataset
   *          short name of data set
   */
  public void setLowRankDataset(String dataset) {
    this.lowRankDataset = dataset;
  }

  /**
   * getViewDataset: Get the data set user viewed
   *
   * @return data set name
   */
  public String getLowRankDataset() {
    return this.lowRankDataset;
  }

  /**
   * setSessionId: Set ID of session
   *
   * @param sessionID
   *          session id
   */
  public void setSessionId(String sessionID) {
    this.sessionID = sessionID;
  }

  /**
   * setType: Set session type name
   *
   * @param type
   *          session type name in elasticsearch
   */
  public void setIndex(String index) {
    this.index = index;
  }

  public void setFilter(Map<String, String> filter) {
    this.filter = filter;
  }

  /**
   * Output click stream info in string format
   *
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "query:" + query + "|| highRankDataset:" + highRankDataset
        + "|| lowRankDataset:" + lowRankDataset;
  }

  /**
   * toJson: Output click stream info in Json format
   *
   * @return session in string format
   */
  public String toJson() {
    String jsonQuery = "{";
    jsonQuery += "\"query\":\"" + this.query + "\",";
    jsonQuery += "\"highRankDataset\":\"" + this.highRankDataset + "\",";
    jsonQuery += "\"lowRankDataset\":\"" + this.lowRankDataset + "\",";

    if (this.filter != null) {
      for (String key : filter.keySet()) {
        jsonQuery += "\"" + key + "\":\"" + filter.get(key) + "\",";
      }
    }

    jsonQuery += "\"sessionId\":\"" + this.sessionID + "\",";
    jsonQuery += "\"index\":\"" + this.index + "\"";
    jsonQuery += "},";
    return jsonQuery;
  }
}
