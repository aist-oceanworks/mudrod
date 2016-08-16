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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
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
 * @author Yongyao
 *
 */
public class Searcher extends MudrodAbstract {
  private static final Logger LOG = LoggerFactory.getLogger(ESDriver.class);
  DecimalFormat NDForm = new DecimalFormat("#.##");

  /**
   * Constructor supporting a number of parameters documented below.
   * @param config a {@link java.util.Map} containing K,V of type String, String respectively.
   * @param es the {@link esiptestbed.mudrod.driver.ESDriver} used to persist log files.
   * @param spark the {@link esiptestbed.mudrod.driver.SparkDriver} used to process input log files.
   */
  public Searcher(Map<String, String> config, ESDriver es, SparkDriver spark) {
    super(config, es, spark);
  }

  public Double getVersionNum(String version)
  {
    if(version==null){
      return 0.0;
    }
    Double versionNum = 0.0;
    Pattern p = Pattern.compile(".*[a-zA-Z].*");
    if(version.equals("Operational/Near-Real-Time"))
    {
      versionNum = 2.0;
    }else if(version.matches("[0-9]{1}[a-zA-Z]{1}"))
    {
      versionNum = Double.parseDouble(version.substring(0, 1));
    }else if(p.matcher(version).find())
    {
      versionNum = 0.0;
    }else
    {
      versionNum = Double.parseDouble(version);
      if(versionNum > 2000)
      {
        versionNum = 4.0;
      }
    }
    return versionNum;
  }
  
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
  public List<SResult> searchByQuery(String index, String type, String query) 
  {
    boolean exists = es.node.client().admin().indices().prepareExists(index).execute().actionGet().isExists();	
    if(!exists){
      return null;
    }

    Dispatcher dp = new Dispatcher(this.getConfig(), this.getES(), null);   
    BoolQueryBuilder qb = dp.createSemQuery(query, 2);
    Map<String, Double> selected_Map = dp.getRelatedTerms(query, 2);
    List<SResult> resultList = new ArrayList<SResult>();

    SearchResponse response = es.client.prepareSearch(index)
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
      Double Dataset_LongName = 0.0;
      if(longName.toLowerCase().trim().contains(query))
      {
        Dataset_LongName = 1.0;
      }

      ArrayList<String> topicList = (ArrayList<String>) result.get("DatasetParameter-Variable");
      String topic = String.join(", ", topicList);
      String content = (String) result.get("Dataset-Description");

      ArrayList<String> longdate = (ArrayList<String>) result.get("DatasetCitation-ReleaseDateLong");
      Date date=new Date(Long.valueOf(longdate.get(0)).longValue());
      SimpleDateFormat df2 = new SimpleDateFormat("MM/dd/yyyy");
      String dateText = df2.format(date);

      //////////////////////////////////////////////more features//////////////////////////////////
      ArrayList<String> metaList = (ArrayList<String>) result.get("Dataset-Metadata");
      Double Dataset_Metadata = exists(metaList, query);
      
      ArrayList<String> termList = (ArrayList<String>) result.get("DatasetParameter-Term");
      Double DatasetParameter_Term = exists(termList, query);
      
      ArrayList<String> sourceList = (ArrayList<String>) result.get("DatasetSource-Source-LongName");
      Double DatasetSource_Source_LongName = exists(sourceList, query);
      
      ArrayList<String> sensorList = (ArrayList<String>) result.get("DatasetSource-Sensor-LongName");
      Double DatasetSource_Sensor_LongName = exists(sensorList, query);
      
      ArrayList<String> versionList = (ArrayList<String>) result.get("DatasetCitation-Version");
      String version=null;
      if(versionList!=null)
      {
        if(versionList.size()>0)
        {
          version = versionList.get(0);
        }
      }

      Double versionNum = getVersionNum(version);
      String processingLevel = (String) result.get("Dataset-ProcessingLevel"); 
      Double proNum = getProLevelNum(processingLevel);

      Double userPop = ((Integer) result.get("Dataset-UserPopularity")).doubleValue();
      Double allPop = ((Integer) result.get("Dataset-AllTimePopularity")).doubleValue();
      Double monthPop = ((Integer) result.get("Dataset-MonthlyPopularity")).doubleValue();
      if(userPop>1000)
      {
        userPop=1000.0;
      }
      if(allPop>1000)
      {
        allPop=1000.0;
      }
      if(monthPop>1000)
      {
        monthPop=1000.0;
      }

      SResult re = new SResult(shortName, longName, topic, content, dateText);
      SResult.set(re, "relevance", relevance);
      
      SResult.set(re, "Dataset_LongName", Dataset_LongName);
      SResult.set(re, "Dataset_Metadata", Dataset_Metadata);
      SResult.set(re, "DatasetParameter_Term", DatasetParameter_Term);
      SResult.set(re, "DatasetSource_Source_LongName", DatasetSource_Source_LongName);
      SResult.set(re, "DatasetSource_Sensor_LongName", DatasetSource_Sensor_LongName);
      
      SResult.set(re, "dateLong", Long.valueOf(longdate.get(0)).doubleValue());     
      SResult.set(re, "version", version);
      SResult.set(re, "versionNum", versionNum);
      SResult.set(re, "processingLevel", processingLevel);
      SResult.set(re, "proNum", proNum);
      SResult.set(re, "userPop", userPop);
      SResult.set(re, "allPop", allPop);
      SResult.set(re, "monthPop", monthPop);

      /***************************set click count*********************************/
      //important to convert shortName to lowercase
      QueryBuilder qb_clicks = dp.createQueryForClicks(query, 2, shortName.toLowerCase()); 
      SearchResponse clicks_res = es.client.prepareSearch(index)
          .setTypes(config.get("clickstreamMatrixType"))            
          .setQuery(qb_clicks)
          .setSize(500)
          .execute()
          .actionGet();

      Double click_count = (double) 0;
      for (SearchHit item : clicks_res.getHits().getHits()) {
        Map<String,Object> click = item.getSource();
        Double click_frequency = Double.parseDouble((String) click.get("clicks"));
        String query_str = (String) click.get("query");
        Double query_weight = selected_Map.get(query_str);
        if(query_weight!=null)
        {
          click_count += click_frequency * query_weight;
        }
      }
      SResult.set(re, "clicks", click_count);
      /****************************Set training label*******************************/
      FilterBuilder filter = FilterBuilders.boolFilter()
          .must(FilterBuilders.termFilter("query", query))
          .must(FilterBuilders.termFilter("dataID", shortName));
      QueryBuilder query_label_search = QueryBuilders
          .filteredQuery(QueryBuilders.matchAllQuery(), filter);
      SearchResponse label_res = es.client.prepareSearch(index)
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
   * @return
   */
  public String ssearch(String index, String type, String query) 
  {
    List<SResult> resultList = searchByQuery(index, type, query);
    Ranker rr = new Ranker(config, es, spark, "pairwise");
    List<SResult> li = rr.rank(resultList);

    Gson gson = new Gson();   
    List<JsonObject> fileList = new ArrayList<>();

    for(int i =0; i< li.size(); i++)
    {
      JsonObject file = new JsonObject();
      file.addProperty("Relevance", String.valueOf((double)SResult.get(li.get(i), "final_score")));
      file.addProperty("Short Name", (String)SResult.get(li.get(i), "shortName"));
      file.addProperty("Long Name", (String)SResult.get(li.get(i), "longName"));
      file.addProperty("Topic", (String)SResult.get(li.get(i), "topic"));
      file.addProperty("Abstract", (String)SResult.get(li.get(i), "description"));
      file.addProperty("Release Date", (String)SResult.get(li.get(i), "date"));
      fileList.add(file);
    }
    JsonElement fileListElement = gson.toJsonTree(fileList);

    JsonObject PDResults = new JsonObject();
    PDResults.add("PDResults", fileListElement);
    return PDResults.toString();
  }
}
