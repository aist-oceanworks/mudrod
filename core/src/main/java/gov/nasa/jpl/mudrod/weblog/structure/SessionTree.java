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

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import gov.nasa.jpl.mudrod.discoveryengine.MudrodAbstract;
import gov.nasa.jpl.mudrod.driver.ESDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * ClassName: SessionTree Function: Convert request list in a session to a tree
 */
public class SessionTree extends MudrodAbstract {

  /**
   *
   */
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(SessionTree.class);
  // size: node numbers in the session tree
  public int size = 0;
  // root: root node of session tree
  protected SessionNode root = null;
  // binsert: indicates inserting a node or not
  public boolean binsert = false;
  // tmpnode: tempt node
  public SessionNode tmpnode;
  // latestDatasetnode: the latest inserted node whose key is "dataset"
  public SessionNode latestDatasetnode;
  // sessionID: session ID
  private String sessionID;
  // cleanupType: session type in Elasticsearch
  private String cleanupType;

  /**
   * Creates a new instance of SessionTree.
   *
   * @param props:       the Mudrod configuration
   * @param es:          the Elasticsearch drive
   * @param rootData:    root node of the tree
   * @param sessionID:   session ID
   * @param cleanupType: session type
   */
  public SessionTree(Properties props, ESDriver es, SessionNode rootData, String sessionID, String cleanupType) {
    super(props, es, null);
    root = new SessionNode("root", "root", "", "", 0);
    tmpnode = root;
    this.sessionID = sessionID;
    this.cleanupType = cleanupType;
  }

  /**
   * Creates a new instance of SessionTree.
   *
   * @param props:       the Mudrod configuration
   * @param es:          the Elasticsearch drive
   * @param sessionID:   session ID
   * @param cleanupType: session type
   */
  public SessionTree(Properties props, ESDriver es, String sessionID, String cleanupType) {
    super(props, es, null);
    root = new SessionNode("root", "root", "", "", 0);
    root.setParent(root);
    tmpnode = root;
    this.sessionID = sessionID;
    this.cleanupType = cleanupType;
  }

  /**
   * insert: insert a node into the session tree.
   *
   * @param node {@link SessionNode}
   * @return session node
   */
  public SessionNode insert(SessionNode node) {
    // begin with datasetlist
    if (node.getKey().equals("datasetlist")) {
      this.binsert = true;
    }
    if (!this.binsert) {
      return null;
    }
    // remove unrelated node
    if (!node.getKey().equals("datasetlist") && !node.getKey().equals("dataset") && !node.getKey().equals("ftp")) {
      return null;
    }
    // remove dumplicated click
    if (node.getRequest().equals(tmpnode.getRequest())) {
      return null;
    }
    // search insert node
    SessionNode parentnode = this.searchParentNode(node);
    if (parentnode == null) {
      return null;
    }
    node.setParent(parentnode);
    parentnode.addChildren(node);

    // record insert node
    tmpnode = node;
    if ("dataset".equals(node.getKey())) {
      latestDatasetnode = node;
    }

    size++;
    return node;
  }

  /**
   * printTree: Print session tree
   *
   * @param node root node of the session tree
   */
  public void printTree(SessionNode node) {
    LOG.info("node: {} \n", node.getRequest());
    if (node.children.isEmpty()) {
      for (int i = 0; i < node.children.size(); i++) {
        printTree(node.children.get(i));
      }
    }
  }

  /**
   * TreeToJson: Convert the session tree to Json object
   *
   * @param node node of the session tree
   * @return tree content in Json format
   */
  public JsonObject treeToJson(SessionNode node) {
    Gson gson = new Gson();
    JsonObject json = new JsonObject();

    json.addProperty("seq", node.getSeq());
    if ("datasetlist".equals(node.getKey())) {
      json.addProperty("icon", "./resources/images/searching.png");
      json.addProperty("name", node.getRequest());
    } else if ("dataset".equals(node.getKey())) {
      json.addProperty("icon", "./resources/images/viewing.png");
      json.addProperty("name", node.getDatasetId());
    } else if ("ftp".equals(node.getKey())) {
      json.addProperty("icon", "./resources/images/downloading.png");
      json.addProperty("name", node.getRequest());
    } else if ("root".equals(node.getKey())) {
      json.addProperty("name", "");
      json.addProperty("icon", "./resources/images/users.png");
    }

    if (!node.children.isEmpty()) {
      List<JsonObject> jsonChildren = new ArrayList<>();
      for (int i = 0; i < node.children.size(); i++) {
        JsonObject jsonChild = treeToJson(node.children.get(i));
        jsonChildren.add(jsonChild);
      }
      JsonElement jsonElement = gson.toJsonTree(jsonChildren);
      json.add("children", jsonElement);
    }

    return json;
  }

  /**
   * getClickStreamList: Get click stream list in the session
   *
   * @return {@link ClickStream}
   */
  public List<ClickStream> getClickStreamList() {

    List<ClickStream> clickthroughs = new ArrayList<>();
    List<SessionNode> viewnodes = this.getViewNodes(this.root);
    for (int i = 0; i < viewnodes.size(); i++) {

      SessionNode viewnode = viewnodes.get(i);
      SessionNode parent = viewnode.getParent();
      List<SessionNode> children = viewnode.getChildren();

      if (!"datasetlist".equals(parent.getKey())) {
        continue;
      }

      RequestUrl requestURL = new RequestUrl();
      String viewquery = "";
      try {
        String infoStr = requestURL.getSearchInfo(viewnode.getRequest());
        viewquery = es.customAnalyzing(props.getProperty("indexName"), infoStr);
      } catch (UnsupportedEncodingException | InterruptedException | ExecutionException e) {
        LOG.warn("Exception getting search info. Ignoring...", e);
      }

      String dataset = viewnode.getDatasetId();
      boolean download = false;
      for (int j = 0; j < children.size(); j++) {
        SessionNode child = children.get(j);
        if ("ftp".equals(child.getKey())) {
          download = true;
          break;
        }
      }

      if (viewquery != null && !"".equals(viewquery)) {
        String[] queries = viewquery.trim().split(",");
        if (queries.length > 0) {
          for (int k = 0; k < queries.length; k++) {
            ClickStream data = new ClickStream(queries[k], dataset, download);
            data.setSessionId(this.sessionID);
            data.setType(this.cleanupType);
            clickthroughs.add(data);
          }
        }
      }
    }

    return clickthroughs;
  }

  /**
   * searchParentNode:Get parent node of a session node
   *
   * @param node {@link SessionNode}
   * @return node {@link SessionNode}
   */
  private SessionNode searchParentNode(SessionNode node) {

    String nodeKey = node.getKey();

    if ("datasetlist".equals(nodeKey)) {
      if ("-".equals(node.getReferer())) {
        return root;
      } else {
        SessionNode tmp = this.findLatestRefer(tmpnode, node.getReferer());
        if (tmp == null) {
          return root;
        } else {
          return tmp;
        }
      }
    } else if ("dataset".equals(nodeKey)) {
      if ("-".equals(node.getReferer())) {
        return null;
      } else {
        return this.findLatestRefer(tmpnode, node.getReferer());
      }
    } else if ("ftp".equals(nodeKey)) {
      return latestDatasetnode;
    }

    return tmpnode;
  }

  /**
   * findLatestRefer: Find parent node whose visiting url is equal to the refer
   * url of a session node
   *
   * @param node:  {@link SessionNode}
   * @param refer: request url
   * @return
   */
  private SessionNode findLatestRefer(SessionNode node, String refer) {
    while (true) {
      if ("root".equals(node.getKey())) {
        return null;
      }
      SessionNode parentNode = node.getParent();
      if (refer.equals(parentNode.getRequest())) {
        return parentNode;
      }

      SessionNode tmp = this.iterChild(parentNode, refer);
      if (tmp == null) {
        node = parentNode;
        continue;
      } else {
        return tmp;
      }
    }
  }

  /**
   * iterChild:
   *
   * @param start
   * @param refer
   * @return
   */
  private SessionNode iterChild(SessionNode start, String refer) {
    List<SessionNode> children = start.getChildren();
    for (int i = children.size() - 1; i >= 0; i--) {
      SessionNode tmp = children.get(i);
      if (tmp.getChildren().isEmpty()) {
        if (refer.equals(tmp.getRequest())) {
          return tmp;
        } else {
          continue;
        }
      } else {
        iterChild(tmp, refer);
      }
    }

    return null;
  }

  /**
   * check:
   *
   * @param children
   * @param str
   * @return
   */
  private boolean check(List<SessionNode> children, String str) {
    for (int i = 0; i < children.size(); i++) {
      if (children.get(i).key.equals(str)) {
        return true;
      }
    }
    return false;
  }

  /**
   * insertHelperChildren:
   *
   * @param entry
   * @param children
   * @return
   */
  private boolean insertHelperChildren(SessionNode entry, List<SessionNode> children) {
    for (int i = 0; i < children.size(); i++) {
      boolean result = insertHelper(entry, children.get(i));
      if (result) {
        return result;
      }
    }
    return false;

  }

  /**
   * insertHelper:
   *
   * @param entry
   * @param node
   * @return
   */
  private boolean insertHelper(SessionNode entry, SessionNode node) {
    if ("datasetlist".equals(entry.key) || "dataset".equals(entry.key)) {
      if ("datasetlist".equals(node.key)) {
        if (node.children.isEmpty()) {
          node.children.add(entry);
          return true;
        } else {
          boolean flag = check(node.children, "datasetlist");
          if (!flag) {
            node.children.add(entry);
            return true;
          } else {
            insertHelperChildren(entry, node.children);
          }
        }
      } else {
        insertHelperChildren(entry, node.children);
      }
    } else if ("ftp".equals(entry.key)) {
      if ("dataset".equals(node.key)) {
        if (node.children.isEmpty()) {
          node.children.add(entry);
          return true;
        } else {
          boolean flag = check(node.children, "dataset");
          if (!flag) {
            node.children.add(entry);
            return true;
          } else {
            insertHelperChildren(entry, node.children);
          }
        }
      } else {
        insertHelperChildren(entry, node.children);
      }
    }

    return false;
  }

  /**
   * getViewNodes: Get a session node's child nodes whose key is "dataset".
   *
   * @param node
   * @return a list of session node
   */
  private List<SessionNode> getViewNodes(SessionNode node) {

    List<SessionNode> viewnodes = new ArrayList<>();
    if ("dataset".equals(node.getKey())) {
      viewnodes.add(node);
    }

    if (!node.children.isEmpty()) {
      for (int i = 0; i < node.children.size(); i++) {
        SessionNode childNode = node.children.get(i);
        viewnodes.addAll(getViewNodes(childNode));
      }
    }

    return viewnodes;
  }

  private List<SessionNode> getQueryNodes(SessionNode node) {
    return this.getNodes(node, "datasetlist");
  }

  private List<SessionNode> getNodes(SessionNode node, String nodeKey) {

    List<SessionNode> nodes = new ArrayList<>();
    if (node.getKey().equals(nodeKey)) {
      nodes.add(node);
    }

    if (!node.children.isEmpty()) {
      for (int i = 0; i < node.children.size(); i++) {
        SessionNode childNode = node.children.get(i);
        nodes.addAll(getNodes(childNode, nodeKey));
      }
    }

    return nodes;
  }

  /**
   * Obtain the ranking training data.
   *
   * @param indexName   the index from whcih to obtain the data
   * @param sessionID   a valid session identifier
   * @return {@link ClickStream}
   * @throws UnsupportedEncodingException if there is an error whilst
   *                                      processing the ranking training data.
   */
  public List<RankingTrainData> getRankingTrainData(String indexName, String sessionID) throws UnsupportedEncodingException {

    List<RankingTrainData> trainDatas = new ArrayList<>();

    List<SessionNode> queryNodes = this.getQueryNodes(this.root);
    for (int i = 0; i < queryNodes.size(); i++) {
      SessionNode querynode = queryNodes.get(i);
      List<SessionNode> children = querynode.getChildren();

      LinkedHashMap<String, Boolean> datasetOpt = new LinkedHashMap<>();
      int ndownload = 0;
      for (int j = 0; j < children.size(); j++) {
        SessionNode node = children.get(j);
        if ("dataset".equals(node.getKey())) {
          Boolean bDownload = false;
          List<SessionNode> nodeChildren = node.getChildren();
          int childSize = nodeChildren.size();
          for (int k = 0; k < childSize; k++) {
            if ("ftp".equals(nodeChildren.get(k).getKey())) {
              bDownload = true;
              ndownload += 1;
              break;
            }
          }
          datasetOpt.put(node.datasetId, bDownload);
        }
      }

      // method 1: The priority of download data are higher
      if (datasetOpt.size() > 1 && ndownload > 0) {
        // query
        RequestUrl requestURL = new RequestUrl();
        String queryUrl = querynode.getRequest();
        String infoStr = requestURL.getSearchInfo(queryUrl);
        String query = null;
        try {
          query = es.customAnalyzing(props.getProperty("indexName"), infoStr);
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException("Error performing custom analyzing", e);
        }
        Map<String, String> filter = RequestUrl.getFilterInfo(queryUrl);

        for (String datasetA : datasetOpt.keySet()) {
          Boolean bDownloadA = datasetOpt.get(datasetA);
          if (bDownloadA) {
            for (String datasetB : datasetOpt.keySet()) {
              Boolean bDownloadB = datasetOpt.get(datasetB);
              if (!bDownloadB) {

                String[] queries = query.split(",");
                for (int l = 0; l < queries.length; l++) {
                  RankingTrainData trainData = new RankingTrainData(queries[l], datasetA, datasetB);

                  trainData.setSessionId(this.sessionID);
                  trainData.setIndex(indexName);
                  trainData.setFilter(filter);
                  trainDatas.add(trainData);
                }
              }
            }
          }
        }
      }
    }

    return trainDatas;
  }
}
