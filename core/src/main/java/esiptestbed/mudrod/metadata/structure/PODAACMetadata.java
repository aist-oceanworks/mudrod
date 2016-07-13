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
package esiptestbed.mudrod.metadata.structure;

import java.io.*;
import java.lang.String;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class PODAACMetadata implements Serializable {

  private String shortname;
  private String abstractStr;
  private String isoTopic;
  private String sensor;
  private String source;
  private String project;
  boolean hasAbstarct;

  private List<String> longnameList;
  private List<String> keywordList;
  private List<String> termList;
  private List<String> topicList;
  private List<String> variableList;
  private List<String> abstractList;
  private List<String> isotopicList;
  private List<String> sensorList;
  private List<String> sourceList;
  private List<String> projectList;

  public PODAACMetadata() {
    // TODO Auto-generated constructor stub
  }

  public PODAACMetadata(String shortname, List<String> longname,
      List<String> topics, List<String> terms, List<String> variables,
      List<String> keywords) throws UnsupportedEncodingException,
      NoSuchAlgorithmException, InterruptedException, ExecutionException {
    this.shortname = shortname;
    this.longnameList = longname;
    this.keywordList = keywords;
    this.termList = terms;
    this.topicList = topics;
    this.variableList = variables;
  }

  public void setTerms(String termstr) {
    this.splitString(termstr, this.termList);
  }

  public void setKeywords(String keywords) {
    this.splitString(keywords, this.keywordList);
  }

  public void setTopicList(String topicStr) {
    this.splitString(topicStr, this.topicList);
  }

  public void setVaraliableList(String varilableStr) {
    this.splitString(varilableStr, this.variableList);
  }

  public void setProjectList(String project2) {
    this.splitString(project2, this.projectList);
  }

  public void setSourceList(String source2) {
    this.splitString(source2, this.sourceList);
  }

  public void setSensorList(String sensor2) {
    this.splitString(sensor2, this.sensorList);
  }

  public void setISOTopicList(String isoTopic2) {
    this.splitString(isoTopic2, this.isotopicList);
  }

  public List<String> getKeywordList() {
    return this.keywordList;
  }

  public List<String> getTermList() {
    return this.termList;
  }

  public String getShortName() {
    return this.shortname;
  }

  public String getKeyword() {
    return String.join(",", this.keywordList);
  }

  public String getTerm() {
    return String.join(",", this.termList);
  }

  public String getTopic() {
    return String.join(",", this.topicList);
  }

  public String getVariable() {
    return String.join(",", this.variableList);
  }

  public String getAbstract() {
    return this.abstractStr;
  }

  public String getProject() {
    // TODO Auto-generated method stub
    return this.project;
  }

  public String getSource() {
    // TODO Auto-generated method stub
    return this.source;
  }

  public String getSensor() {
    // TODO Auto-generated method stub
    return this.sensor;
  }

  public String getISOTopic() {
    // TODO Auto-generated method stub
    return this.isoTopic;
  }

  public List<String> getAllTermList()
      throws InterruptedException, ExecutionException {
    List<String> allterms = new ArrayList<String>();

    if (this.termList != null && this.termList.size() > 0) {
      allterms.addAll(this.termList);
    }

    if (this.keywordList != null && this.keywordList.size() > 0) {
      allterms.addAll(this.keywordList);
    }

    if (this.topicList != null && this.topicList.size() > 0) {
      allterms.addAll(this.topicList);
    }

    if (this.variableList != null && this.variableList.size() > 0) {
      allterms.addAll(this.variableList);
    }

    // reserved
    /*
     * if (this.isotopicList.size() > 0) { allterms.addAll(this.isotopicList); }
     * 
     * if (this.sensorList.size() > 0) { allterms.addAll(this.sensorList); } if
     * (this.sourceList.size() > 0) { allterms.addAll(this.sourceList); } if
     * (this.projectList.size() > 0) { allterms.addAll(this.projectList); } if
     * (this.abstractList.size() > 0) { allterms.addAll(this.abstractList); }
     */

    return allterms;
  }

  private void splitString(String oristr, List<String> list) {
    if (oristr == null) {
      return;
    }

    int length = oristr.length();
    if (oristr.startsWith("\"")) {
      oristr = oristr.substring(1);
    }
    if (oristr.endsWith("\"")) {
      oristr = oristr.substring(0, oristr.length() - 1);
    }

    String strs[] = oristr.trim().split(",");
    if (strs != null) {
      for (int i = 0; i < strs.length; i++) {
        String str = strs[i].trim();
        if (str.startsWith(",") || str.startsWith("\"")) {
          str = str.substring(1);
        }
        if (str.endsWith(",") || str.endsWith("\"")) {
          str = str.substring(0, str.length() - 1);
        }
        if (str == "") {
          continue;
        }
        list.add(str);
      }
    }
  }

  private void splitAbstract(String abstractStr) {
    Set<String> set = new HashSet<String>(this.termList);
    set.addAll(this.topicList);
    set.addAll(this.variableList);
    set.addAll(this.keywordList);
    List<String> mergeList = new ArrayList<String>(set);
  }
}
