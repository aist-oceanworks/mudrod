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
import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import esiptestbed.mudrod.discoveryengine.MudrodAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.main.MudrodEngine;
import esiptestbed.mudrod.ssearch.ranking.AttributeExtractor;
import esiptestbed.mudrod.ssearch.structure.SResult;

/**
 * Supports ability to performance semantic search with a given query
 */
public class Searcher extends MudrodAbstract implements Serializable {
  DecimalFormat NDForm = new DecimalFormat("#.##");
  final Integer MAX_CHAR = 700;
  AttributeExtractor attEx = new AttributeExtractor();

  public Searcher(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  /**
   * Main method of semantic search
   *
   * @param index
   *          index name in Elasticsearch
   * @param type
   *          type name in Elasticsearch
   * @param query
   *          regular query string
   * @param query_operator
   *          query mode- query, or, and
   * @return a list of search result
   */
  @SuppressWarnings("unchecked")
  public List<SResult> searchByQuery(String index, String type, String query,
      String query_operator) {
    boolean exists = es.getClient().admin().indices().prepareExists(index)
        .execute().actionGet().isExists();
    if (!exists) {
      return new ArrayList<SResult>();
    }

    Dispatcher dp = new Dispatcher(this.getConfig(), this.getES(), null);
    BoolQueryBuilder qb = dp.createSemQuery(query, 1.0, query_operator);
    List<SResult> resultList = new ArrayList<SResult>();

    SearchResponse response = es.getClient().prepareSearch(index).setTypes(type)
        .setQuery(qb).setSize(500).execute().actionGet();

    for (SearchHit hit : response.getHits().getHits()) {
      Map<String, Object> result = hit.getSource();
      Double relevance = Double.valueOf(NDForm.format(hit.getScore()));
      String shortName = (String) result.get("Dataset-ShortName");
      String longName = (String) result.get("Dataset-LongName");

      ArrayList<String> topicList = (ArrayList<String>) result
          .get("DatasetParameter-Variable");
      String topic = String.join(", ", topicList);
      String content = (String) result.get("Dataset-Description");

      if (!content.equals("")) {
        int maxLength = (content.length() < MAX_CHAR) ? content.length()
            : MAX_CHAR;
        content = content.trim().substring(0, maxLength - 1) + "...";
      }

      ArrayList<String> longdate = (ArrayList<String>) result
          .get("DatasetCitation-ReleaseDateLong");
      Date date = new Date(Long.valueOf(longdate.get(0)).longValue());
      SimpleDateFormat df2 = new SimpleDateFormat("MM/dd/yyyy");
      String dateText = df2.format(date);

      ArrayList<String> versionList = (ArrayList<String>) result.get("DatasetCitation-Version");
      String version=null;
      if(versionList!=null)
      {
        if(versionList.size()>0)
        {
          version = versionList.get(0);
        }
      }
      Double versionNum = attEx.getVersionNum(version);
      String processingLevel = (String) result.get("Dataset-ProcessingLevel");
      Double proNum = attEx.getProLevelNum(processingLevel);

      Double userPop = attEx.getPop(
          ((Integer) result.get("Dataset-UserPopularity")).doubleValue());
      Double allPop = attEx.getPop(
          ((Integer) result.get("Dataset-AllTimePopularity")).doubleValue());
      Double monthPop = attEx.getPop(
          ((Integer) result.get("Dataset-MonthlyPopularity")).doubleValue());
      Double spatialR = attEx.getSpatialR(result);
      Double temporalR = attEx.getTemporalR(result);

      SResult re = new SResult(shortName, longName, topic, content, dateText);
      SResult.set(re, "term", relevance);
      SResult.set(re, "releaseDate",
          Long.valueOf(longdate.get(0)).doubleValue());
      SResult.set(re, "versionNum", versionNum);
      SResult.set(re, "processingLevel", processingLevel);
      SResult.set(re, "processingL", proNum);
      SResult.set(re, "userPop", userPop);
      SResult.set(re, "allPop", allPop);
      SResult.set(re, "monthPop", monthPop);
      SResult.set(re, "spatialR", spatialR);
      SResult.set(re, "temporalR", temporalR);

      QueryBuilder query_label_search = QueryBuilders.boolQuery()
          .must(QueryBuilders.termQuery("query", query))
          .must(QueryBuilders.termQuery("dataID", shortName));
      SearchResponse label_res = es.getClient().prepareSearch(index)
          .setTypes("trainingranking").setQuery(query_label_search).setSize(5)
          .execute().actionGet();
      String label_string = null;
      for (SearchHit label : label_res.getHits().getHits()) {
        Map<String, Object> label_item = label.getSource();
        label_string = (String) label_item.get("label");
      }
      SResult.set(re, "label", label_string);
      /***************************************/
      resultList.add(re);
    }

    return resultList;
  }

  /**
   * Method of semantic search to generate JSON string
   *
   * @param index
   *          index name in Elasticsearch
   * @param type
   *          type name in Elasticsearch
   * @param query
   *          regular query string
   * @param query_operator
   *          query mode- query, or, and
   * @param rr
   *          selected ranking method
   * @return search results
   */
  public String ssearch(String index, String type, String query,
      String query_operator, Ranker rr) {
    List<SResult> resultList = searchByQuery(index, type, query,
        query_operator);
    List<SResult> li = rr.rank(resultList);
    Gson gson = new Gson();
    List<JsonObject> fileList = new ArrayList<>();

    for (int i = 0; i < li.size(); i++) {
      JsonObject file = new JsonObject();
      file.addProperty("Short Name",
          (String) SResult.get(li.get(i), "shortName"));
      file.addProperty("Long Name",
          (String) SResult.get(li.get(i), "longName"));
      file.addProperty("Topic", (String) SResult.get(li.get(i), "topic"));
      file.addProperty("Abstract",
          (String) SResult.get(li.get(i), "description"));
      file.addProperty("Release Date",
          (String) SResult.get(li.get(i), "relase_date"));
      fileList.add(file);
    }
    JsonElement fileListElement = gson.toJsonTree(fileList);

    JsonObject PDResults = new JsonObject();
    PDResults.add("PDResults", fileListElement);
    return PDResults.toString();
  }


  public static void main(String[] args) throws IOException {
    String learnerType = "SparkSVM";

    String[] query_list = {"ocean wind", "ocean temperature", "sea surface topography", "quikscat",
        "saline density", "pathfinder", "gravity", "ocean pressure", "radar", "sea ice"};
    //String[] query_list = {"ocean wind"};

    MudrodEngine me = new MudrodEngine();
    Properties props = me.loadConfig();
    me.setESDriver(new ESDriver(props));
    me.setSparkDriver(new SparkDriver());

    Searcher sr = new Searcher(me.getConfig(), me.getESDriver(), null);
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
          me.getConfig().getProperty("raw_metadataType"), q, "phrase");
      Ranker rr = new Ranker(me.getConfig(), me.getESDriver(), me.getSparkDriver(), learnerType);
      List<SResult> li = rr.rank(resultList);
      for(int i =0; i< li.size(); i++)
      {
        bw.write(li.get(i).toString(","));
      }
      bw.close();
    }

    me.end();   
  }
}
