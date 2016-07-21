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

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import esiptestbed.mudrod.discoveryengine.MudrodAbstract;
import esiptestbed.mudrod.driver.ESDriver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionTree extends MudrodAbstract {

  private static final Logger LOG = LoggerFactory.getLogger(SessionTree.class);
  public int size = 0;
  protected SessionNode root = null;
  public boolean binsert = false;
  public SessionNode tmpnode;
  public SessionNode latestDatasetnode;
  private String sessionID;
  private String cleanupType;

  public SessionTree(Map<String, String> config, ESDriver es,
      SessionNode rootData, String sessionID, String cleanupType) {
    super(config, es, null);
    root = new SessionNode("root", "root", "", "", 0);
    tmpnode = root;
    this.sessionID = sessionID;
    this.cleanupType = cleanupType;
  }

  public SessionTree(Map<String, String> config, ESDriver es, String sessionID,
      String cleanupType) {
    super(config, es, null);
    root = new SessionNode("root", "root", "", "", 0);
    root.setParent(root);
    tmpnode = root;
    this.sessionID = sessionID;
    this.cleanupType = cleanupType;
  }

  public SessionNode insert(SessionNode node) {
    // begin with datasetlist
    if (node.getKey().equals("datasetlist")) {
      this.binsert = true;
    }
    if (!this.binsert) {
      return null;
    }
    // remove unrelated node
    if (!node.getKey().equals("datasetlist") && !node.getKey().equals("dataset")
        && !node.getKey().equals("ftp")) {
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
    if (node.getKey().equals("dataset")) {
      latestDatasetnode = node;
    }

    size++;
    return node;
  }

  public void printTree(SessionNode node) {
    LOG.info("node: {} \n", node.getRequest());
    if (node.children.size() != 0) {
      for (int i = 0; i < node.children.size(); i++) {
        printTree(node.children.get(i));
      }
    }
  }

  public JsonObject TreeToJson(SessionNode node) {
    Gson gson = new Gson();
    JsonObject json = new JsonObject();

    json.addProperty("seq", node.getSeq());
    if ("datasetlist".equals(node.getKey())) {
      json.addProperty("icon", "searching.png");

      json.addProperty("name", node.getRequest());

    } else if ("dataset".equals(node.getKey())) {
      json.addProperty("icon", "viewing.png");
      json.addProperty("name", node.getDatasetId());
    } else if ("ftp".equals(node.getKey())) {
      json.addProperty("icon", "downloading.png");
      json.addProperty("name", node.getRequest());
    } else if ("root".equals(node.getKey())) {
      json.addProperty("name", "");
      json.addProperty("icon", "users.png");
    }

    if (node.children.isEmpty()) {
      List<JsonObject> jsonChildren = new ArrayList<>();
      for (int i = 0; i < node.children.size(); i++) {
        JsonObject jsonChild = TreeToJson(node.children.get(i));
        jsonChildren.add(jsonChild);
      }
      JsonElement jsonElement = gson.toJsonTree(jsonChildren);
      json.add("children", jsonElement);
    }

    return json;
  }

  public List<ClickStream> getClickStreamList()
      throws UnsupportedEncodingException {

    List<ClickStream> clickthroughs = new ArrayList<>();
    List<SessionNode> viewnodes = this.getViewNodes(this.root);
    for (int i = 0; i < viewnodes.size(); i++) {

      SessionNode viewnode = viewnodes.get(i);
      SessionNode parent = viewnode.getParent();
      List<SessionNode> children = viewnode.getChildren();

      if (!parent.getKey().equals("datasetlist")) {
        continue;
      }

      RequestUrl requestURL = new RequestUrl(this.config, this.es, null);
      String viewquery = requestURL.GetSearchInfo(viewnode.getRequest());

      String dataset = viewnode.getDatasetId();
      boolean download = false;
      for (int j = 0; j < children.size(); j++) {
        SessionNode child = children.get(j);
        if ("ftp".equals(child.getKey())) {
          download = true;
          break;
        }
      }

      if (viewquery != null && !viewquery.equals("")) {
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

  private SessionNode insert(String key, String value) {
    SessionNode entry = new SessionNode();
    entry.key = key;
    entry.value = value;
    if (root == null) {
      root = entry;
    } else {
      insertHelper(entry, root);
    }
    size++;
    return entry;
  }

  private SessionNode searchParentNode(SessionNode node) {

    String tmpnodeKey = tmpnode.getKey();
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
      if (node.getReferer().equals("-")) {
        return null;
      } else {
        return this.findLatestRefer(tmpnode, node.getReferer());
      }
    } else if (nodeKey.equals("ftp")) {
      return latestDatasetnode;
    }

    return tmpnode;
  }

  private SessionNode findLatestRefer(SessionNode start, String refer) {
    while (true) {
      if (start.getKey().equals("root")) {
        return null;
      }
      start = start.getParent();
      if (refer.equals(start.getRequest())) {
        return start;
      }

      SessionNode tmp = this.iterChild(start, refer);
      if (tmp == null) {
        continue;
      } else {
        return tmp;
      }
    }
  }

  private SessionNode iterChild(SessionNode start, String refer) {
    List<SessionNode> children = start.getChildren();
    SessionNode tmp = null;
    for (int i = children.size() - 1; i >= 0; i--) {
      tmp = children.get(i);
      if (tmp.getChildren().size() == 0) {
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

  private boolean check(List<SessionNode> children, String str) {
    for (int i = 0; i < children.size(); i++) {
      if (children.get(i).key.equals(str)) {
        return true;
      }
    }
    return false;
  }

  private boolean insertHelperChildren(SessionNode entry,
      List<SessionNode> children) {
    for (int i = 0; i < children.size(); i++) {
      boolean result = insertHelper(entry, children.get(i));
      if (result == true) {
        return result;
      }
    }
    return false;

  }

  private boolean insertHelper(SessionNode entry, SessionNode node) {
    if (entry.key.equals("datasetlist") || entry.key.equals("dataset")) {
      if (node.key.equals("datasetlist")) {
        if (node.children.size() == 0) {
          node.children.add(entry);
          return true;
        } else {
          boolean flag = check(node.children, "datasetlist");
          if (flag == false) {
            node.children.add(entry);
            return true;
          } else {
            insertHelperChildren(entry, node.children);
          }
        }
      } else {
        insertHelperChildren(entry, node.children);
      }
    } else if (entry.key.equals("ftp")) {
      if (node.key.equals("dataset")) {
        if (node.children.size() == 0) {
          node.children.add(entry);
          return true;
        } else {
          boolean flag = check(node.children, "dataset");
          if (flag == false) {
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

  private List<SessionNode> getViewNodes(SessionNode node) {

    List<SessionNode> viewnodes = new ArrayList<SessionNode>();
    if (node.getKey().equals("dataset")) {
      viewnodes.add(node);
    }

    if (node.children.size() != 0) {
      for (int i = 0; i < node.children.size(); i++) {
        SessionNode childNode = node.children.get(i);
        viewnodes.addAll(getViewNodes(childNode));
      }
    }

    return viewnodes;
  }
}
