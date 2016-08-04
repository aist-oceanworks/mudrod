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

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.index.query.QueryBuilder;
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
   * @param config  Hashmap read from config.xml
   * @param es      Elasticsearch client
   * @param spark   Spark connection
   */
  public Searcher(Map<String, String> config, ESDriver es, SparkDriver spark) {
    super(config, es, spark);
    // TODO Auto-generated constructor stub
  }
  /**
   * 
   * @param index index name in elasticsearch
   * @param type  type name in elasticsearch
   * @param query query string
   * @return a list of result
   */
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
      @SuppressWarnings("unchecked")
      ArrayList<String> topicList = (ArrayList<String>) result.get("DatasetParameter-Variable");
      String topic = String.join(", ", topicList);
      String content = (String) result.get("Dataset-Description");
      @SuppressWarnings("unchecked")
      ArrayList<String> longdate = (ArrayList<String>) result.get("DatasetCitation-ReleaseDateLong");

      Date date=new Date(Long.valueOf(longdate.get(0)).longValue());
      SimpleDateFormat df2 = new SimpleDateFormat("MM/dd/yyyy");
      String dateText = df2.format(date);

      //more features
      String latency = (String) result.get("DatasetPolicy-DataLatency");
      @SuppressWarnings("unchecked")
      ArrayList<String> versionList = (ArrayList<String>) result.get("DatasetCitation-Version");
      String version=null;
      if(versionList!=null)
      {
        if(versionList.size()>0)
        {
          version = versionList.get(0);
        }
      }
      
      String stopDateLong = (String) result.get("Dataset-DatasetCoverage-StopTimeLong");
      if(stopDateLong.equals(""))
      {
        stopDateLong = String.valueOf(System.currentTimeMillis());
      }
      Date date1=new Date(Long.valueOf(stopDateLong).longValue());
      String dateText1 = df2.format(date1);
      
      String processingLevel = (String) result.get("Dataset-ProcessingLevel");     
      Double spatialR = (Double) result.get("Dataset-SatelliteSpatialResolution");
      String temporalR = (String) result.get("Dataset-TemporalResolution");
      Integer userPop = (Integer) result.get("Dataset-UserPopularity");
      Integer allPop = (Integer) result.get("Dataset-AllTimePopularity");
      if(allPop>1000)
      {
        allPop=1000;
      }
      
      Integer monthPop = (Integer) result.get("Dataset-MonthlyPopularity");


      SResult re = new SResult(shortName, longName, topic, content, dateText);
      SResult.set(re, "relevance", relevance);
      SResult.set(re, "dateLong", Long.valueOf(longdate.get(0)).longValue());     
      SResult.set(re, "version", version);
      SResult.set(re, "processingLevel", processingLevel);
      SResult.set(re, "latency", latency);
      SResult.set(re, "stopDateLong", stopDateLong);
      SResult.set(re, "stopDateFormat", dateText1);
      SResult.set(re, "spatialR", spatialR);
      SResult.set(re, "temporalR", temporalR);
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
      //re.setClicks(click_count);
      /***************************************************************************/
      resultList.add(re); 
    }

    return resultList;
  }

  public String ssearch(String index, String type, String query) 
  {
    List<SResult> resultList = searchByQuery(index, type, query);
    Ranker rr = new Ranker(config, es, spark);
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

  public static void main(String[] args) throws IOException {
    String query = "ocean wind";
    File file = new File("C:/mudrodCoreTestData/rankingResults/" + query + ".csv");
    if (file.exists()) {
      file.delete();
    }

    file.createNewFile();

    FileWriter fw = new FileWriter(file.getAbsoluteFile());
    BufferedWriter bw = new BufferedWriter(fw); 
    bw.write( "Query:"+ query + "\n");
    bw.write(SResult.getHeader(","));

    MudrodEngine mudrod = new MudrodEngine("Elasticsearch");
    Searcher sr = new Searcher(mudrod.getConfig(), mudrod.getES(), null);
    List<SResult> resultList = sr.searchByQuery(mudrod.getConfig().get("indexName"), 
        mudrod.getConfig().get("raw_metadataType"), query);
    Ranker rr = new Ranker(mudrod.getConfig(), mudrod.getES(), null);
    List<SResult> li = rr.rank(resultList);
    for(int i =0; i< li.size(); i++)
    {
      //System.out.println(li.get(i).toString(" "));
      bw.write(li.get(i).toString(","));
    }

    bw.close();
    /*  String result = sr.ssearch(mudrod.getConfig().get("indexName"), 
        mudrod.getConfig().get("raw_metadataType"), 
        "sea surface temperature");
    System.out.println(result);*/
    mudrod.end();   
  }
}
