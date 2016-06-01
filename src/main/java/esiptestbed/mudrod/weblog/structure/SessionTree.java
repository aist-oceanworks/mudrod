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


public class SessionTree extends MudrodAbstract{
  public int size = 0;
  protected TreeNode root = null;
  public boolean binsert = false;
  public TreeNode tmpnode;
  public TreeNode latestDatasetnode;

  public SessionTree(Map<String, String> config, ESDriver es,TreeNode rootData) {
    super(config, es);
    root = new TreeNode("root", "root", "", "", 0);
    tmpnode = root;
  }

  public SessionTree(Map<String, String> config, ESDriver es) {
    super(config, es);
    root = new TreeNode("root", "root", "", "", 0);
    root.setParent(root);
    tmpnode = root;
  }

  public int size() {
    return size;
  }

  public TreeNode insert(TreeNode node) {
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
    TreeNode parentnode = this.searchParentNode(node);
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

  public TreeNode searchParentNode(TreeNode node) {

    String tmpnodeKey = tmpnode.getKey();
    String nodeKey = node.getKey();

    if (nodeKey.equals("datasetlist")) {
      if (node.getReferer().equals("-")) {
        return root;
      } else {
        TreeNode tmp = this.findLatestRefer(tmpnode, node.getReferer());
        if (tmp == null) {
          return root;
        } else {
          return tmp;
        }
      }
    } else if (nodeKey.equals("dataset")) {
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

  public TreeNode findLatestRefer(TreeNode start, String refer) {
    /*
     * if (refer.equals(start.getRequest())) { return start; }
     */
    while (true) {
      if (start.getKey().equals("root")) {
        return null;
      }
      start = start.getParent();
      if (refer.equals(start.getRequest())) {
        return start;
      }

      TreeNode tmp = this.iterChild(start, refer);
      if (tmp == null) {
        continue;
      } else {
        return tmp;
      }
    }
  }

  public TreeNode iterChild(TreeNode start, String refer) {
    List<TreeNode> children = start.getChildren();
    TreeNode tmp = null;
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

  public TreeNode insert(String key, String value) {
    TreeNode entry = new TreeNode();
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

  private boolean check(List<TreeNode> children, String str) {
    for (int i = 0; i < children.size(); i++) {
      if (children.get(i).key.equals(str)) {
        return true;
      }
    }
    return false;
  }

  private boolean insertHelperChildren(TreeNode entry, List<TreeNode> children) {
    for (int i = 0; i < children.size(); i++) {
      boolean result = insertHelper(entry, children.get(i));
      if (result == true) {
        return result;
      }
    }
    return false;

  }

  private boolean insertHelper(TreeNode entry, TreeNode node) {
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

  public void printTree(TreeNode node) {
    System.out.print("node:" + node.getRequest() + "\n");
    if (node.children.size() != 0) {
      for (int i = 0; i < node.children.size(); i++) {
        printTree(node.children.get(i));
      }
    }
  }

  public JsonObject TreeToJson(TreeNode node) {
    Gson gson = new Gson();
    JsonObject json = new JsonObject();

    json.addProperty("seq", node.getSeq());
    if (node.getKey().equals("datasetlist")) {
      json.addProperty("icon", "searching.png");


      json.addProperty("name", node.getRequest());

    } else if (node.getKey().equals("dataset")) {
      json.addProperty("icon", "viewing.png");
      json.addProperty("name", node.getDatasetId());
      // json.addProperty("tips", node.getDatasetId());

    } else if (node.getKey().equals("ftp")) {
      json.addProperty("icon", "downloading.png");
      json.addProperty("name", node.getRequest());
      // json.addProperty("tips", node.getRequest());

    } else if (node.getKey().equals("root")) {
      json.addProperty("name", "");
      json.addProperty("icon", "users.png");
      // json.addProperty("tips","user");
    }

    if (node.children.size() != 0) {
      List<JsonObject> json_children = new ArrayList<JsonObject>();
      for (int i = 0; i < node.children.size(); i++) {
        JsonObject json_child = TreeToJson(node.children.get(i));
        json_children.add(json_child);
      }
      JsonElement jsonElement = gson.toJsonTree(json_children);
      json.add("children", jsonElement);
    }

    return json;
  }

  public List<ClickThroughData> genClickThroughData(List<TreeNode> viewnodes) throws UnsupportedEncodingException {

    List<ClickThroughData> clickthroughs = new ArrayList<ClickThroughData>();

    for (int i = 0; i < viewnodes.size(); i++) {

      TreeNode viewnode = viewnodes.get(i);
      TreeNode parent = viewnode.getParent();
      List<TreeNode> children = viewnode.getChildren();

      if (!parent.getKey().equals("datasetlist")) {
        continue;
      }

      RequestUrl requestURL = new RequestUrl(this.config, this.es);
      String viewquery = requestURL.GetSearchInfo(viewnode.getRequest());

      String dataset = viewnode.getDatasetId();
      boolean download = false;
      for (int j = 0; j < children.size(); j++) {
        TreeNode child = children.get(j);
        if (child.getKey().equals("ftp")) {
          download = true;
          break;
        }
      }

      //lemmanization -- add by yun
      if (!viewquery.equals("")) {

        ClickThroughData data = new ClickThroughData(viewquery, dataset, download);
        clickthroughs.add(data);
      }
    }

    return clickthroughs;
  }

  public List<KeywordPairData> genKeywordPairData(List<TreeNode> downloadnodes) throws UnsupportedEncodingException {

    List<KeywordPairData> keywordPairs = new ArrayList<KeywordPairData>();

    List<TreeNode> viewNodes = new ArrayList<TreeNode>();
    List<TreeNode> searchNodes = new ArrayList<TreeNode>();

    for (int i = 0; i < downloadnodes.size(); i++) {

      TreeNode downloadNode = downloadnodes.get(i);
      TreeNode parent = downloadNode.getParent();
      if (viewNodes.contains(parent)) {
        continue;
      }
      viewNodes.add(parent);

      while (true) {
        if (parent.getKey().equals("datasetlist")) {
          searchNodes.add(parent);
        }
        parent = parent.getParent();

        if (parent.getKey().equals("root")) {
          break;
        }
      }

      // System.out.print("searchNodes:"+ searchNodes.size()+"\n");
      RequestUrl requestURL = new RequestUrl(this.config, this.es);
      if (searchNodes.size() > 1) {

        for (int k = 0; k < searchNodes.size(); k++) {
          int size = searchNodes.size();
          TreeNode lastSearch = searchNodes.get(k);
          String lastquery = requestURL.GetSearchInfo(lastSearch.getRequest());
          lastquery = lastquery.replace(",", "|");
          for (int j = k; j < size; j++) {
            String curquery = requestURL.GetSearchInfo(searchNodes.get(j).getRequest());
            curquery = curquery.replace(",", "|");
            if (!curquery.equals(lastquery)) {
              KeywordPairData data = new KeywordPairData(curquery, lastquery, 1.0 / (k + 1));
              keywordPairs.add(data);
            }
          }
        }
      }
    }

    return keywordPairs;
  }



  public List<KeywordPairData> genQueryFilterPairs(List<TreeNode> viewnodes) throws UnsupportedEncodingException {
    List<KeywordPairData> keywordPairs = new ArrayList<KeywordPairData>();
    for (int i = 0; i < viewnodes.size(); i++) {

      TreeNode viewNode = viewnodes.get(i);
      TreeNode lastSearchNode = viewNode.getParent();
      while (true) {
        if (lastSearchNode.getKey().equals("datasetlist")) {
          break;
        }
        lastSearchNode = lastSearchNode.getParent();
      }

      String lastquery = RequestUrl.GetSearchWord(lastSearchNode.getRequest());
      Map<String, String> lastfitler = RequestUrl.GetFilterInfo(lastSearchNode.getRequest());

      if (lastfitler.keySet().size() == 0) {
        continue;
      }
      KeywordPairData data = null;
      if (!lastquery.equals("")) {
        for (String s : lastfitler.keySet()) {
          if (!lastquery.equals(lastfitler.get(s)) && !lastfitler.get(s).equals("")) {
            data = new KeywordPairData(lastquery, s + ":" + lastfitler.get(s), 1.0, 0,
                lastSearchNode.getRequest(), lastSearchNode.getRequest(), viewNode.getRequest());
            keywordPairs.add(data);
          }
        }

        String filterStr = "";
        for (String s : lastfitler.keySet()) {
          if(!lastfitler.get(s).equals("")){
            filterStr += s + ":" + lastfitler.get(s) + ",";
          }
        }
        data = new KeywordPairData(lastquery, filterStr, 1.0, 1, lastSearchNode.getRequest(),
            lastSearchNode.getRequest(), viewNode.getRequest());
        keywordPairs.add(data);
      }

      for (String lastkey : lastfitler.keySet()) {
        if(lastfitler.get(lastkey).equals("")){
          continue;
        }

        if (lastkey.equals("processinglevel") || lastkey.equals("gridspatialresolution")
            || lastkey.equals("temporalresolution") || lastkey.equals("timespan")
            || lastkey.equals("temporalsearch") || lastkey.equals("satellitespatialresolution")) {
          continue;
        }

        for (String key : lastfitler.keySet()) {
          if (!key.equals(lastkey) && !lastfitler.get(key).equals("")) {
            data = new KeywordPairData(lastfitler.get(lastkey), key + ":" + lastfitler.get(key), 1.0, 0,
                lastSearchNode.getRequest(), lastSearchNode.getRequest(), viewNode.getRequest());
            keywordPairs.add(data);
          }
        }

        String filterStr = "";
        for (String key : lastfitler.keySet()) {
          if (!key.equals(lastkey)) {
            if(!lastfitler.get(key).equals("")){
              filterStr += key + ":" + lastfitler.get(key) + ",";
            }
          }
        }
        data = new KeywordPairData(lastfitler.get(lastkey), filterStr, 1.0, 1, lastSearchNode.getRequest(),
            lastSearchNode.getRequest(), viewNode.getRequest());
        keywordPairs.add(data);
      }
    }

    return keywordPairs;
  }

  public List<TreeNode> getViewNodes(TreeNode node) {

    List<TreeNode> viewnodes = new ArrayList<TreeNode>();
    if (node.getKey().equals("dataset")) {
      viewnodes.add(node);
    }

    if (node.children.size() != 0) {
      for (int i = 0; i < node.children.size(); i++) {
        TreeNode childNode = node.children.get(i);
        viewnodes.addAll(getViewNodes(childNode));
      }
    }

    return viewnodes;
  }

  public List<TreeNode> getTreeLeaves(TreeNode node) {

    List<TreeNode> leaves = new ArrayList<TreeNode>();
    if (node.children.size() != 0) {
      for (int i = 0; i < node.children.size(); i++) {
        TreeNode childNode = node.children.get(i);
        leaves.addAll(getTreeLeaves(childNode));
      }
    } else {
      leaves.add(node);
    }

    return leaves;
  }

  public List<TreeNode> getDownLoadNodes(TreeNode node) {

    List<TreeNode> leaves = new ArrayList<TreeNode>();
    if (node.children.size() != 0) {
      for (int i = 0; i < node.children.size(); i++) {
        TreeNode childNode = node.children.get(i);
        leaves.addAll(getDownLoadNodes(childNode));
      }
    } else {

      if (node.getKey().equals("ftp")) {
        leaves.add(node);
      }
    }

    return leaves;
  }
}
