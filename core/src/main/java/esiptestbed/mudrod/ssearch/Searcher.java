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
package esiptestbed.mudrod.ssearch;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import esiptestbed.mudrod.discoveryengine.MudrodAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.main.MudrodEngine;
import esiptestbed.mudrod.ssearch.structure.SResult;

/**
 * Supports ability to performance semantic search with a given query
 */
public class Searcher extends MudrodAbstract implements Serializable{
  private static final Logger LOG = LoggerFactory.getLogger(ESDriver.class);
  DecimalFormat NDForm = new DecimalFormat("#.##");
  final Integer MAX_CHAR = 700;

  public Searcher(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  /**
   * Method of converting processing level string into a number
   * @param pro processing level string
   * @return processing level number
   */
  public Double getProLevelNum(String pro){
    if(pro==null){
      return 1.0;
    }
    Double proNum = 0.0;
    Pattern p = Pattern.compile(".*[a-zA-Z].*");
    if(pro.matches("[0-9]{1}[a-zA-Z]{1}"))
    {
      proNum = Double.parseDouble(pro.substring(0, 1));
    }else if(p.matcher(pro).find())
    {
      proNum = 1.0;
    }else
    {
      proNum = Double.parseDouble(pro);
    }

    return proNum;
  }

  public Double getPop(Double pop)
  {
    if(pop>1000)
    {
      pop=1000.0;
    }
    return pop;
  }

  /**
   * Method of checking if query exists in a certain attribute
   * @param strList attribute value in the form of ArrayList
   * @param query query string
   * @return 1 means query exists, 0 otherwise
   */
  public Double exists(ArrayList<String> strList, String query)
  {
    Double val = 0.0;
    if(strList!=null)
    {
      String str = String.join(", ", strList);
      if(str!=null&&str.length()!=0)
      {
        if(str.toLowerCase().trim().contains(query))
        {
          val = 1.0;
        }
      }
    }
    return val;
  }

  /**
   * Main method of semantic search
   * @param index index name in Elasticsearch
   * @param type  type name in Elasticsearch
   * @param query regular query string
   * @return a list of search result
   */
  @SuppressWarnings("unchecked")
  public List<SResult> searchByQuery(String index, String type, String query, String query_operator) 
  {
    boolean exists = es.getClient().admin().indices().prepareExists(index).execute().actionGet().isExists();  
    if(!exists){
      return null;
    }

    Dispatcher dp = new Dispatcher(this.getConfig(), this.getES(), null);   
    BoolQueryBuilder qb = dp.createSemQuery(query, 1.0, query_operator);
    List<SResult> resultList = new ArrayList<SResult>();

    SearchResponse response = es.getClient().prepareSearch(index)
        .setTypes(type)           
        .setQuery(qb)
        .setSize(500)
        .execute()
        .actionGet();

    for (SearchHit hit : response.getHits().getHits()) {
      Map<String,Object> result = hit.getSource();
      Double relevance = Double.valueOf(NDForm.format(hit.getScore()));
      String shortName = (String) result.get("Dataset-ShortName");
      String longName = (String) result.get("Dataset-LongName");

      ArrayList<String> topicList = (ArrayList<String>) result.get("DatasetParameter-Variable");
      String topic = String.join(", ", topicList);
      String content = (String) result.get("Dataset-Description");

      if(!content.equals(""))
      {
        int maxLength = (content.length() < MAX_CHAR)?content.length():MAX_CHAR;
        content = content.trim().substring(0, maxLength-1) + "...";
      }

      ArrayList<String> longdate = (ArrayList<String>) result.get("DatasetCitation-ReleaseDateLong");
      Date date = new Date(Long.valueOf(longdate.get(0)).longValue());
      SimpleDateFormat df2 = new SimpleDateFormat("MM/dd/yyyy");
      String dateText = df2.format(date);

      //////////////////////////////////////////////more features//////////////////////////////////
      String processingLevel = (String) result.get("Dataset-ProcessingLevel"); 
      Double proNum = getProLevelNum(processingLevel);

      Double userPop = getPop(((Integer) result.get("Dataset-UserPopularity")).doubleValue());
      Double allPop = getPop(((Integer) result.get("Dataset-AllTimePopularity")).doubleValue());
      Double monthPop = getPop(((Integer) result.get("Dataset-MonthlyPopularity")).doubleValue());

      SResult re = new SResult(shortName, longName, topic, content, dateText);

      SResult.set(re, "term", relevance);
      SResult.set(re, "releaseDate", Long.valueOf(longdate.get(0)).doubleValue());     
      SResult.set(re, "processingLevel", processingLevel);
      SResult.set(re, "processingL", proNum);
      SResult.set(re, "userPop", userPop);
      SResult.set(re, "allPop", allPop);
      SResult.set(re, "monthPop", monthPop);

      QueryBuilder query_label_search = QueryBuilders.boolQuery()
          .must(QueryBuilders.termQuery("query", query))
          .must(QueryBuilders.termQuery("dataID", shortName));
      SearchResponse label_res = es.getClient().prepareSearch(index)
          .setTypes("trainingranking")            
          .setQuery(query_label_search)
          .setSize(5)
          .execute()
          .actionGet();
      String label_string = null;
      for (SearchHit label : label_res.getHits().getHits()) {
        Map<String,Object> label_item = label.getSource();
        label_string = (String) label_item.get("label");
      }
      SResult.set(re, "label", label_string);
      /***************************************************************************/
      resultList.add(re); 
    }

    return resultList;
  }

  /**
   * Method of semantic search to generate JSON string
   * @param index index name in Elasticsearch
   * @param type  type name in Elasticsearch
   * @param query regular query string
   * @param rr selected ranking method
   * @return search results
   */
  public String ssearch(String index, String type, String query, String query_operator, Ranker rr) 
  {
    List<SResult> resultList = searchByQuery(index, type, query, query_operator);
    List<SResult> li = rr.rank(resultList);
    Gson gson = new Gson();   
    List<JsonObject> fileList = new ArrayList<>();

    for(int i =0; i< li.size(); i++)
    {
      JsonObject file = new JsonObject();
      file.addProperty("Short Name", (String)SResult.get(li.get(i), "shortName"));
      file.addProperty("Long Name", (String)SResult.get(li.get(i), "longName"));
      file.addProperty("Topic", (String)SResult.get(li.get(i), "topic"));
      file.addProperty("Abstract", (String)SResult.get(li.get(i), "description"));
      file.addProperty("Release Date", (String)SResult.get(li.get(i), "relase_date"));
      fileList.add(file);
    }
    JsonElement fileListElement = gson.toJsonTree(fileList);

    JsonObject PDResults = new JsonObject();
    PDResults.add("PDResults", fileListElement);
    return PDResults.toString();
  }
}
