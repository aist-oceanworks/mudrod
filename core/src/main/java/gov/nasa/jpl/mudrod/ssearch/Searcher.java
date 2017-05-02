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
package gov.nasa.jpl.mudrod.ssearch;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import gov.nasa.jpl.mudrod.discoveryengine.MudrodAbstract;
import gov.nasa.jpl.mudrod.driver.ESDriver;
import gov.nasa.jpl.mudrod.driver.SparkDriver;
import gov.nasa.jpl.mudrod.ssearch.structure.SResult;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Supports ability to performance semantic search with a given query
 */
public class Searcher extends MudrodAbstract implements Serializable {
  DecimalFormat NDForm = new DecimalFormat("#.##");
  final Integer MAX_CHAR = 700;
  private static final String DS_PARAM_VAR = "DatasetParameter-Variable";

  public Searcher(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  /**
   * Method of converting processing level string into a number
   *
   * @param pro processing level string
   * @return processing level number
   */
  public Double getProLevelNum(String pro) {
    if (pro == null) {
      return 1.0;
    }
    Double proNum;
    Pattern p = Pattern.compile(".*[a-zA-Z].*");
    if (pro.matches("[0-9]{1}[a-zA-Z]{1}")) {
      proNum = Double.parseDouble(pro.substring(0, 1));
    } else if (p.matcher(pro).find()) {
      proNum = 1.0;
    } else {
      proNum = Double.parseDouble(pro);
    }

    return proNum;
  }

  public Double getPop(Double pop) {
    if (pop > 1000) {
      pop = 1000.0;
    }
    return pop;
  }

  /**
   * Method of checking if query exists in a certain attribute
   *
   * @param strList attribute value in the form of ArrayList
   * @param query   query string
   * @return 1 means query exists, 0 otherwise
   */
  public Double exists(ArrayList<String> strList, String query) {
    Double val = 0.0;
    if (strList != null) {
      String str = String.join(", ", strList);
      if (str != null && str.length() != 0 && str.toLowerCase().trim().contains(query)) {
        val = 1.0;
      }
    }
    return val;
  }

  /**
   * Main method of semantic search
   *
   * @param index          index name in Elasticsearch
   * @param type           type name in Elasticsearch
   * @param query          regular query string
   * @param query_operator query mode- query, or, and
   * @return a list of search result
   */
  @SuppressWarnings("unchecked")
  public List<SResult> searchByQuery(String index, String type, String query, String query_operator) {
    boolean exists = es.getClient().admin().indices().prepareExists(index).execute().actionGet().isExists();
    if (!exists) {
      return new ArrayList<SResult>();
    }

    Dispatcher dp = new Dispatcher(this.getConfig(), this.getES(), null);
    BoolQueryBuilder qb = dp.createSemQuery(query, 1.0, query_operator);
    List<SResult> resultList = new ArrayList<SResult>();

    SearchResponse response = es.getClient().prepareSearch(index).setTypes(type).setQuery(qb).setSize(500).execute().actionGet();

    for (SearchHit hit : response.getHits().getHits()) {
      Map<String, Object> result = hit.getSource();
      Double relevance = Double.valueOf(NDForm.format(hit.getScore()));
      String shortName = (String) result.get("Dataset-ShortName");
      String longName = (String) result.get("Dataset-LongName");

      ArrayList<String> topicList = (ArrayList<String>) result.get("DatasetParameter-Variable");
      String topic = String.join(", ", topicList);
      String content = (String) result.get("Dataset-Description");

      if (!content.equals("")) {
        int maxLength = (content.length() < MAX_CHAR) ? content.length() : MAX_CHAR;
        content = content.trim().substring(0, maxLength - 1) + "...";
      }

      ArrayList<String> longdate = (ArrayList<String>) result.get("DatasetCitation-ReleaseDateLong");
      Date date = new Date(Long.valueOf(longdate.get(0)).longValue());
      SimpleDateFormat df2 = new SimpleDateFormat("MM/dd/yyyy");
      String dateText = df2.format(date);

      //start date
      Long start = (Long) result.get("DatasetCoverage-StartTimeLong-Long");
      Date startDate = new Date(start);
      String startDateTxt = df2.format(startDate);

      //end date
      String end = (String) result.get("Dataset-DatasetCoverage-StopTimeLong");
      String endDateTxt = "";
      if (end.equals("")) {
        endDateTxt = "now";
      } else {
        Date endDate = new Date(Long.valueOf(end));
        endDateTxt = df2.format(endDate);
      }

      String processingLevel = (String) result.get("Dataset-ProcessingLevel");
      Double proNum = getProLevelNum(processingLevel);

      Double userPop = getPop(((Integer) result.get("Dataset-UserPopularity")).doubleValue());
      Double allPop = getPop(((Integer) result.get("Dataset-AllTimePopularity")).doubleValue());
      Double monthPop = getPop(((Integer) result.get("Dataset-MonthlyPopularity")).doubleValue());

      List<String> sensors = (List<String>) result.get("DatasetSource-Sensor-ShortName");

      SResult re = new SResult(shortName, longName, topic, content, dateText);

      SResult.set(re, "term", relevance);
      SResult.set(re, "releaseDate", Long.valueOf(longdate.get(0)).doubleValue());
      SResult.set(re, "processingLevel", processingLevel);
      SResult.set(re, "processingL", proNum);
      SResult.set(re, "userPop", userPop);
      SResult.set(re, "allPop", allPop);
      SResult.set(re, "monthPop", monthPop);
      SResult.set(re, "startDate", startDateTxt);
      SResult.set(re, "endDate", endDateTxt);
      SResult.set(re, "sensors", String.join(", ", sensors));

      QueryBuilder query_label_search = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("query", query)).must(QueryBuilders.termQuery("dataID", shortName));
      SearchResponse label_res = es.getClient().prepareSearch(index).setTypes("trainingranking").setQuery(query_label_search).setSize(5).execute().actionGet();
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
   * @param index          index name in Elasticsearch
   * @param type           type name in Elasticsearch
   * @param query          regular query string
   * @param query_operator query mode- query, or, and
   * @param rr             selected ranking method
   * @return search results
   */
  public String ssearch(String index, String type, String query, String query_operator, Ranker rr) {
    List<SResult> resultList = searchByQuery(index, type, query, query_operator);
    List<SResult> li = rr.rank(resultList);
    Gson gson = new Gson();
    List<JsonObject> fileList = new ArrayList<>();

    for (int i = 0; i < li.size(); i++) {
      JsonObject file = new JsonObject();
      file.addProperty("Short Name", (String) SResult.get(li.get(i), "shortName"));
      file.addProperty("Long Name", (String) SResult.get(li.get(i), "longName"));
      file.addProperty("Topic", (String) SResult.get(li.get(i), "topic"));
      file.addProperty("Description", (String) SResult.get(li.get(i), "description"));
      file.addProperty("Release Date", (String) SResult.get(li.get(i), "relase_date"));
      fileList.add(file);

      file.addProperty("Start/End Date", (String) SResult.get(li.get(i), "startDate") + " - " + (String) SResult.get(li.get(i), "endDate"));
      file.addProperty("Processing Level", (String) SResult.get(li.get(i), "processingLevel"));

      file.addProperty("Sensor", (String) SResult.get(li.get(i), "sensors"));
    }
    JsonElement fileListElement = gson.toJsonTree(fileList);

    JsonObject PDResults = new JsonObject();
    PDResults.add("PDResults", fileListElement);
    return PDResults.toString();
  }
  
  public String metadataDetails(String index, String type, String query, Boolean bDetail){
	  boolean exists = es.getClient().admin().indices().prepareExists(index).execute().actionGet().isExists();
	    if (!exists) {
	      return null;
	    }

	    QueryBuilder qb = QueryBuilders.queryStringQuery(query);
	    SearchResponse response = es.getClient().prepareSearch(index).setTypes(type).setQuery(qb).setSize(500).execute().actionGet();

	    Gson gson = new Gson();
	    List<JsonObject> fileList = new ArrayList<>();

	    for (SearchHit hit : response.getHits().getHits()) {
	      Map<String, Object> result = hit.getSource();
	      String shortName = (String) result.get("Dataset-ShortName");
	      String longName = (String) result.get("Dataset-LongName");
	      ArrayList<String> topicList = (ArrayList<String>) result.get(DS_PARAM_VAR);
	      String topic = String.join(", ", topicList);
	      String content = (String) result.get("Dataset-Description");

	      ArrayList<String> longdate = (ArrayList<String>) result.get("DatasetCitation-ReleaseDateLong");

	      Date date = new Date(Long.parseLong(longdate.get(0)));
	      SimpleDateFormat df2 = new SimpleDateFormat("dd/MM/yy");
	      String dateText = df2.format(date);

	      JsonObject file = new JsonObject();
	      file.addProperty("Short Name", shortName);
	      file.addProperty("Long Name", longName);
	      file.addProperty("Topic", topic);
	      file.addProperty("Dataset-Description", content);
	      file.addProperty("Release Date", dateText);

	      if (bDetail) {    	  
	    	file.addProperty("DataFormat", (String) result.get("DatasetPolicy-DataFormat"));  
	    	file.addProperty("Dataset-Doi", (String) result.get("Dataset-Doi"));
	        file.addProperty("Processing Level",
	            (String) result.get("Dataset-ProcessingLevel"));
	        
	        List<String> versions = (List<String>) result
	                .get("DatasetCitation-Version");
	        file.addProperty("Version",  String.join(", ", versions));

	        List<String> sensors = (List<String>) result
	            .get("DatasetSource-Sensor-ShortName");
	        file.addProperty("DatasetSource-Sensor-ShortName",
	            String.join(", ", sensors));

	        List<String> projects = (List<String>) result
	            .get("DatasetProject-Project-ShortName");
	        file.addProperty("DatasetProject-Project-ShortName",
	            String.join(", ", projects));

	        List<String> categories = (List<String>) result
	            .get("DatasetParameter-Category");
	        file.addProperty("DatasetParameter-Category",
	            String.join(", ", categories));
	        
	        List<String> urls = (List<String>) result
	            .get("DatasetLocationPolicy-BasePath");
	        
	        List<String> filtered_urls = new ArrayList<String>();
	        for(String url:urls)
	        {
	          if(url.startsWith("ftp")||url.startsWith("http"))
	            filtered_urls.add(url);
	        }
	        file.addProperty("DatasetLocationPolicy-BasePath",
	            String.join(",", filtered_urls));

	        List<String> variables = (List<String>) result
	            .get(DS_PARAM_VAR);

	        file.addProperty(DS_PARAM_VAR, String.join(", ", variables));

	        List<String> terms = (List<String>) result.get("DatasetParameter-Term");
	        file.addProperty("DatasetParameter-Term", String.join(", ", terms));

	        /********coverage*********/

	        List<String> region = (List<String>) result.get("DatasetRegion-Region");
	        file.addProperty("Region", String.join(", ", region));

	        String northLat = (String) result.get("DatasetCoverage-NorthLat");
	        String southLat = (String) result.get("DatasetCoverage-SouthLat");
	        String westLon = (String) result.get("DatasetCoverage-WestLon");
	        String eastLon = (String) result.get("DatasetCoverage-EastLon");
	        file.addProperty("Coverage",
	            northLat + " (northernmost latitude) x " + southLat + " (southernmost latitude) x " + westLon + " (westernmost longitude) x " + eastLon + " (easternmost longitude)");

	        //start date
	        Long start = (Long) result.get("DatasetCoverage-StartTimeLong-Long");
	        Date startDate = new Date(start);
	        String startDateTxt = df2.format(startDate);

	        //end date
	        String end = (String) result.get("Dataset-DatasetCoverage-StopTimeLong");
	        String endDateTxt = "";
	        if (end.equals("")) {
	          endDateTxt = "Present";
	        } else {
	          Date endDate = new Date(Long.valueOf(end));
	          endDateTxt = df2.format(endDate);
	        }
	        file.addProperty("Time Span", startDateTxt + " to " + endDateTxt);
	        /********coverage*********/

	        /********resolution*********/
	        //temporal
	        String temporalResolution = (String) result.get("Dataset-TemporalResolution");
	        if ("".equals(temporalResolution)) {
	          temporalResolution = (String) result.get("Dataset-TemporalRepeat");
	        }
	        file.addProperty("TemporalResolution", temporalResolution);

	        //spatial
	        String latResolution = (String) result.get("Dataset-LatitudeResolution");
	        String lonResolution = (String) result.get("Dataset-LongitudeResolution");
	        if (!latResolution.isEmpty() && !lonResolution.isEmpty()) {
	          file.addProperty("SpatiallResolution", latResolution + " degrees (latitude) x " + lonResolution + " degrees (longitude)");
	        } else {

	          String acrossResolution = (String) result.get("Dataset-AcrossTrackResolution");
	          String alonResolution = (String) result.get("Dataset-AlongTrackResolution");

	          file.addProperty("SpatiallResolution", alonResolution + " m (Along) x " + acrossResolution + " m (Across)");
	        }
	      }

	      fileList.add(file);

	    }
	    JsonElement fileListElement = gson.toJsonTree(fileList);

	    JsonObject pdResults = new JsonObject();
	    pdResults.add("PDResults", fileListElement);
	    return pdResults.toString();
  }
}
