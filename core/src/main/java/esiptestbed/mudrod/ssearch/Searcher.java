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
import esiptestbed.mudrod.ssearch.ranking.Evaluator;
import esiptestbed.mudrod.ssearch.structure.SResult;

/**
 * Supports ability to performance semantic search with a given query
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
  public Searcher(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  /**
   * Method of extracting version number from version string
   * @param version version string
   * @return version number
   */
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
      /*if(versionNum >=5)
      {
        versionNum = 20.0;
      }*/
    }
    return versionNum;
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
  public List<SResult> searchByQuery(String index, String type, String query) 
  {
    boolean exists = es.getClient().admin().indices().prepareExists(index).execute().actionGet().isExists();  
    if(!exists){
      return null;
    }

    Dispatcher dp = new Dispatcher(this.getConfig(), this.getES(), null);   
    BoolQueryBuilder qb = dp.createSemQuery(query, 1.0);
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

      ArrayList<String> longdate = (ArrayList<String>) result.get("DatasetCitation-ReleaseDateLong");
      Date date=new Date(Long.valueOf(longdate.get(0)).longValue());
      SimpleDateFormat df2 = new SimpleDateFormat("MM/dd/yyyy");
      String dateText = df2.format(date);

      //////////////////////////////////////////////more features//////////////////////////////////
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

      Double userPop = getPop(((Integer) result.get("Dataset-UserPopularity")).doubleValue());
      Double allPop = getPop(((Integer) result.get("Dataset-AllTimePopularity")).doubleValue());
      Double monthPop = getPop(((Integer) result.get("Dataset-MonthlyPopularity")).doubleValue());

      SResult re = new SResult(shortName, longName, topic, content, dateText);
      
      Double versionFactor = Math.log(versionNum + 1);
      SResult.set(re, "term", relevance);
      SResult.set(re, "termAndv", relevance + versionFactor);
      
      SResult.set(re, "releaseDate", Long.valueOf(longdate.get(0)).doubleValue());     
      SResult.set(re, "version", version);
      SResult.set(re, "versionNum", versionNum);
      SResult.set(re, "processingLevel", processingLevel);
      SResult.set(re, "processingL", proNum);
      SResult.set(re, "userPop", userPop);
      SResult.set(re, "allPop", allPop);
      SResult.set(re, "monthPop", monthPop);

      /*FilterBuilder filter = FilterBuilders.boolFilter()
          .must(FilterBuilders.termFilter("query", query))
          .must(FilterBuilders.termFilter("dataID", shortName));
      QueryBuilder query_label_search = QueryBuilders
          .filteredQuery(QueryBuilders.matchAllQuery(), filter);*/
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
   * @return
   */
  public String ssearch(String index, String type, String query, Ranker rr) 
  {
    List<SResult> resultList = searchByQuery(index, type, query);
    //Ranker rr = new Ranker(config, es, spark, "pairwise");
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
  
  public int getRankNum(String str)
  {
    switch (str) {
    case "Excellent":
      return 5;
    case "Good":
      return 4;
    case "OK":
      return 3;
    case "Bad":
      return 2;
    case "Terrible":
      return 1;
    default:
      break;
    }
    return 0;
  }
  
  public static void main(String[] args) throws IOException {
    String learnerType = "pairwise";
    String[] query_list = {"ocean waves", "ocean pressure", "sea ice", "radar",
        "ocean optics", "spurs"};
    Evaluator eva = new Evaluator();
    
    MudrodEngine me = new MudrodEngine();
    me.loadConfig();
    me.setES(new ESDriver(me.getConfig()));
    
    Searcher sr = new Searcher(me.getConfig(), me.getES(), null);
    for(String q:query_list)
    {
      File file = new File("C:/mudrodCoreTestData/rankingResults/test/" + q + "_" + learnerType + ".csv");
      if (file.exists()) {
        file.delete();
      }

      file.createNewFile();

      FileWriter fw = new FileWriter(file.getAbsoluteFile());
      BufferedWriter bw = new BufferedWriter(fw); 
      bw.write(SResult.getHeader(","));

      List<SResult> resultList = sr.searchByQuery(me.getConfig().getProperty("indexName"), 
          me.getConfig().getProperty("raw_metadataType"), q);
      Ranker rr = new Ranker(me.getConfig(), me.getES(), null, learnerType);
      List<SResult> li = rr.rank(resultList);
      int[] rank_array = new int[li.size()];
      for(int i =0; i< li.size(); i++)
      {
        bw.write(li.get(i).toString(","));
        //rank_array[i] = sr.getRankNum(li.get(i).label);
      }
      /*double precision = eva.getPrecision(rank_array, 200);
      double ndcg = eva.getNDCG(rank_array, 200);     
      System.out.println("precision and ndcg of " + q + ": " + precision + ", " + ndcg);*/

      bw.close();
    }

    me.end();   
  }
}
