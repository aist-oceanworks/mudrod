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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * ClassName: PODAACMetadata Function: PODAACMetadata setter and getter methods
 * Date: Aug 12, 2016 11:16:43 AM
 *
 * @author Yun
 *
 */
public class PODAACMetadata implements Serializable {

  // shortname: data set short name
  private String shortname;
  // abstractStr: data set abstract
  private String abstractStr;
  // isoTopic: data set topic
  private String isoTopic;
  // sensor: sensor
  private String sensor;
  // source: data source
  private String source;
  // project: data project
  private String project;
  // hasAbstarct: whether data set has abstract
  boolean hasAbstarct;

  // longnameList: data set long name list
  private List<String> longnameList;
  // keywordList:data set key word list
  private List<String> keywordList;
  // termList: data set term list
  private List<String> termList;
  // topicList: data set topic list
  private List<String> topicList;
  // variableList: data set variable list
  private List<String> variableList;
  // abstractList: data set abstract term list
  private List<String> abstractList;
  // isotopicList: data set iso topic list
  private List<String> isotopicList;
  // sensorList: data set sensor list
  private List<String> sensorList;
  // sourceList: data set source list
  private List<String> sourceList;
  // projectList: data set project list
  private List<String> projectList;

  public PODAACMetadata() {
    // TODO Auto-generated constructor stub
  }

  /**
   * Creates a new instance of PODAACMetadata.
   *
   * @param shortname
   *          data set short name
   * @param longname
   *          data set long name
   * @param topics
   *          data set topics
   * @param terms
   *          data set terms
   * @param variables
   *          data set variables
   * @param keywords
   *          data set keywords
   */
  public PODAACMetadata(String shortname, List<String> longname,
      List<String> topics, List<String> terms, List<String> variables,
      List<String> keywords) {
    this.shortname = shortname;
    this.longnameList = longname;
    this.keywordList = keywords;
    this.termList = terms;
    this.topicList = topics;
    this.variableList = variables;
  }

  /**
   * setTerms: set term of data set
   *
   * @param termstr
   *          data set terms
   */
  public void setTerms(String termstr) {
    this.splitString(termstr, this.termList);
  }

  /**
   * setKeywords: set key word of data set
   *
   * @param keywords
   *          data set keywords
   */
  public void setKeywords(String keywords) {
    this.splitString(keywords, this.keywordList);
  }

  /**
   * setTopicList: set topic of data set
   *
   * @param topicStr
   *          data set topics
   */
  public void setTopicList(String topicStr) {
    this.splitString(topicStr, this.topicList);
  }

  /**
   * setVaraliableList: set varilable of data set
   *
   * @param varilableStr
   *          data set variables
   */
  public void setVaraliableList(String varilableStr) {
    this.splitString(varilableStr, this.variableList);
  }

  /**
   * setProjectList:set project of data set
   *
   * @param project
   *          data set projects
   */
  public void setProjectList(String project) {
    this.splitString(project, this.projectList);
  }

  /**
   * setSourceList: set source of data set
   *
   * @param source
   *          data set sources
   */
  public void setSourceList(String source) {
    this.splitString(source, this.sourceList);
  }

  /**
   * setSensorList: set sensor of data set
   *
   * @param sensor
   *          data set sensors
   */
  public void setSensorList(String sensor) {
    this.splitString(sensor, this.sensorList);
  }

  /**
   * setISOTopicList:set iso topic of data set
   *
   * @param isoTopic
   *          data set iso topics
   */
  public void setISOTopicList(String isoTopic) {
    this.splitString(isoTopic, this.isotopicList);
  }

  /**
   * getKeywordList: get key word of data set
   *
   * @return data set keyword list
   */
  public List<String> getKeywordList() {
    return this.keywordList;
  }

  /**
   * getTermList:get term list of data set
   *
   * @return data set term list
   */
  public List<String> getTermList() {
    return this.termList;
  }

  /**
   * getShortName:get short name of data set
   *
   * @return data set short name
   */
  public String getShortName() {
    return this.shortname;
  }

  /**
   * getKeyword:get key word of data set
   *
   * @return data set keyword string
   */
  public String getKeyword() {
    return String.join(",", this.keywordList);
  }

  /**
   * getTerm:get term of data set
   *
   * @return data set term string
   */
  public String getTerm() {
    return String.join(",", this.termList);
  }

  /**
   * getTopic:get topic of data set
   *
   * @return data set topic string
   */
  public String getTopic() {
    return String.join(",", this.topicList);
  }

  /**
   * getVariable:get variable of data set
   *
   * @return data set variable string
   */
  public String getVariable() {
    return String.join(",", this.variableList);
  }

  /**
   * getAbstract:get abstract of data set
   *
   * @return data set abstract
   */
  public String getAbstract() {
    return this.abstractStr;
  }

  /**
   * getProject:get project of data set
   *
   * @return data set project string
   */
  public String getProject() {
    // TODO Auto-generated method stub
    return this.project;
  }

  /**
   * getSource:get source of data set
   *
   * @return data set source string
   */
  public String getSource() {
    // TODO Auto-generated method stub
    return this.source;
  }

  /**
   * getSensor:get sensor of data set
   *
   * @return data set sensor string
   */
  public String getSensor() {
    // TODO Auto-generated method stub
    return this.sensor;
  }

  /**
   * getISOTopic:get iso topic of data set
   *
   * @return data set ISO topic string
   */
  public String getISOTopic() {
    // TODO Auto-generated method stub
    return this.isoTopic;
  }

  /**
   * getAllTermList: get all term list of data set
   *
   * @return data set term list
   */
  public List<String> getAllTermList() {
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

  /**
   * splitString: split value of fields of data set
   *
   * @param oristr
   *          original string
   * @param list
   *          result after splitting
   */
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

  /**
   * splitAbstract: split abstract of data set
   */
  private void splitAbstract(String abstractStr) {
    Set<String> set = new HashSet<String>(this.termList);
    set.addAll(this.topicList);
    set.addAll(this.variableList);
    set.addAll(this.keywordList);
    List<String> mergeList = new ArrayList<String>(set);
  }
}
