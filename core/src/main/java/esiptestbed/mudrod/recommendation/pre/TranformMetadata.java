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
package esiptestbed.mudrod.recommendation.pre;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;

public class TranformMetadata extends DiscoveryStepAbstract {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory
      .getLogger(TranformMetadata.class);

  private String indexName;
  private List<String> CategoricalVars;
  private Map<String, Map<String, String>> CategoricalVarValueFacets;

  private String metadataType;
  private String VAR_NOT_EXIST = "varNotExist";

  public TranformMetadata(Map<String, String> config, ESDriver es,
      SparkDriver spark) {
    super(config, es, spark);

    indexName = config.get("indexName");
    metadataType = config.get("recom_metadataType");
    CategoricalVarValueFacets = new HashMap<String, Map<String, String>>();
    CategoricalVars = new ArrayList<String>();

    CategoricalVars.add("Dataset-ProcessingLevel");
    CategoricalVars.add("DatasetCoverage-NorthLat");
    CategoricalVars.add("DatasetCoverage-SouthLat");
    CategoricalVars.add("DatasetCoverage-WestLon");
    CategoricalVars.add("DatasetCoverage-EastLon");

  }

  @Override
  public Object execute() {
    // TODO Auto-generated method stub
    LOG.info(
        "*****************Mapping metadata variable value starts******************");

    startTime = System.currentTimeMillis();
    // this.TransformVars();
    // this.TranformAllMetadata();

    this.TermExtractorAllMetadata(es);

    endTime = System.currentTimeMillis();
    es.refreshIndex();

    LOG.info(
        "*****************Mapping metadata variable value ends******************Took {}s",
        (endTime - startTime) / 1000);
    return null;
  }

  @Override
  public Object execute(Object o) {
    // TODO Auto-generated method stub
    return null;
  }

  private void TranformAllMetadata() {
    es.createBulkProcesser();

    SearchResponse scrollResp = es.client.prepareSearch(config.get("indexName"))
        .setTypes(metadataType).setScroll(new TimeValue(60000))
        .setQuery(QueryBuilders.matchAllQuery()).setSize(100).execute()
        .actionGet();
    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> metadata = hit.getSource();
        Map<String, Object> metadatacode;
        try {
          metadatacode = TranformMetadata(metadata);
          UpdateRequest ur = es.genUpdateRequest(config.get("indexName"),
              metadataType, hit.getId(), metadatacode);
          es.bulkProcessor.add(ur);
        } catch (InterruptedException | ExecutionException e1) {
          // TODO Auto-generated catch block
          e1.printStackTrace();
        }
      }

      scrollResp = es.client.prepareSearchScroll(scrollResp.getScrollId())
          .setScroll(new TimeValue(600000)).execute().actionGet();
      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }
    }

    es.destroyBulkProcessor();
  }

  private Map<String, Object> TranformMetadata(Map<String, Object> metadata)
      throws InterruptedException, ExecutionException {
    String code = "";
    Map<String, Object> metadataCodes = new HashMap<String, Object>();
    int CategoryNum = CategoricalVars.size();
    for (int i = 0; i < CategoryNum; i++) {
      String var = CategoricalVars.get(i);
      // System.out.println(var);
      String groups = null;
      if (metadata.get(var) != null && metadata.get(var) != "") {
        String value = es.customAnalyzing(config.get("indexName"), "csv",
            metadata.get(var).toString());
        groups = getValueGroups(var, value);
      } else {
        groups = getValueGroups(var, VAR_NOT_EXIST);
      }

      metadataCodes.put(var + "_facet", groups);
    }

    return metadataCodes;
  }

  private String getValueGroups(String var, String value) {
    String groups = "";
    if (value.contains(",")) {
      String[] values = value.split(",");
      String tmpGroup = "";
      int valuenum = values.length;
      for (int j = 0; j < valuenum; j++) {
        tmpGroup = getValueGroup(var, values[j]);
        if (groups == "") {
          groups = tmpGroup;
        } else {
          groups += "," + tmpGroup;
        }
      }
    } else {
      groups = getValueGroup(var, value);
      if (groups == "") {
        groups = getValueGroup(var, VAR_NOT_EXIST);
      }
    }

    return groups;
  }

  private String getValueGroup(String var, String value) {

    if (value.startsWith("[")) {
      value = value.substring(1, value.length());
    }

    if (value.endsWith("]")) {
      value = value.substring(0, value.length() - 1);
    }

    String tmpvec = CategoricalVarValueFacets.get(var).get(value);
    if (tmpvec == "") {
      tmpvec = CategoricalVarValueFacets.get(var).get(VAR_NOT_EXIST);
    }
    return tmpvec;
  }

  private void TransformVars() {
    int CategoryNum = CategoricalVars.size();
    for (int i = 0; i < CategoryNum; i++) {
      String var = CategoricalVars.get(i);
      Map<String, String> valueVecs = this.TranformVar(var);
      // System.out.println(var + " ： " + valueVecs.toString());
      CategoricalVarValueFacets.put(var, valueVecs);
    }
  }

  private Map<String, String> TranformVar(String varName) {

    SearchResponse sr = es.client.prepareSearch(config.get("indexName"))
        .setTypes(config.get("recom_metadataType"))
        .setQuery(QueryBuilders.matchAllQuery()).setSize(0)
        .addAggregation(
            AggregationBuilders.terms("Values").field(varName).size(0))
        .execute().actionGet();
    Terms VarValues = sr.getAggregations().get("Values");

    Map<String, String> valueGroup = tranformValueToCategory(varName,
        VarValues);

    return valueGroup;
  }

  private Map<String, String> tranformValueToCategory(String varName,
      Terms VarValues) {
    List<String> values = new ArrayList<String>();
    Map<String, String> valueGroup = new HashMap<String, String>();
    for (Terms.Bucket entry : VarValues.getBuckets()) {
      String value = entry.getKey();
      values.add(value);
    }

    int size = values.size();
    switch (varName) {
    case "Dataset-ProcessingLevel":
      for (int i = 0; i < size; i++) {
        valueGroup.put(values.get(i), values.get(i).substring(0, 1));
      }
      // System.out.println(valueGroup);
      break;
    case "DatasetCoverage-NorthLat":
    case "DatasetCoverage-SouthLat":
      for (int i = 0; i < size; i++) {
        double lat = Double.parseDouble(values.get(i));
        int group = (int) (lat / 10) * 10;
        valueGroup.put(values.get(i), Integer.toString(group));
      }
      break;
    case "DatasetCoverage-WestLon":
    case "DatasetCoverage-EastLon":
      for (int i = 0; i < size; i++) {
        double lon = Double.parseDouble(values.get(i));
        if (lon > 180) {
          lon = lon - 360;
        }
        int group = (int) (lon / 10) * 10;
        valueGroup.put(values.get(i), Integer.toString(group));
      }
      break;
    default:
      for (int i = 0; i < size; i++) {
        valueGroup.put(values.get(i), values.get(i));
      }
      break;
    }

    return valueGroup;
  }

  public void TermExtractorAllMetadata(ESDriver es) {

    es.createBulkProcesser();

    /* SearchResponse scrollResp = es.client.prepareSearch(indexName)
        .setTypes(metadataType).setScroll(new TimeValue(60000))
        .setQuery(QueryBuilders.matchAllQuery()).setSize(100).execute()
        .actionGet();
    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> metadata = hit.getSource();
        Map<String, Object> extractedTermMap = new HashMap<String, Object>();
        try {

          String dataset = (String) metadata.get("Dataset-ShortName");
          String dataAbstract = (String) metadata.get("Dataset-Description");
          String longname = (String) metadata.get("Dataset-LongName");

          if (metadata.containsKey("extractedTerms")) {

            String extractedTerm = (String) metadata.get("extractedTerms");
            if (!extractedTerm.equals("")) {
              continue;
            }

          }

          String oriShorname = dataset.replaceAll("-", " ").replaceAll("_",
              " ");
          dataAbstract = es.customAnalyzing(indexName, dataAbstract);
          dataAbstract = dataAbstract.replaceAll("\"", "");
          String oriTxt = longname + " " + dataAbstract + " " + oriShorname;
          String extractedTerm = termExtractor(dataset, oriTxt);
          extractedTermMap.put("extractedTerms", extractedTerm);
          UpdateRequest ur = es.genUpdateRequest(indexName, metadataType,
              hit.getId(), extractedTermMap);
          es.bulkProcessor.add(ur);
        } catch (Exception e1) {
          // TODO Auto-generated catch block
          e1.printStackTrace();
        }
      }

      scrollResp = es.client.prepareSearchScroll(scrollResp.getScrollId())
          .setScroll(new TimeValue(600000)).execute().actionGet();
      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }
    }*/

    SearchResponse scrollResp = es.client.prepareSearch(indexName)
        .setTypes(metadataType).setScroll(new TimeValue(60000))
        .setQuery(QueryBuilders.matchAllQuery()).setSize(200).execute()
        .actionGet();

    for (SearchHit hit : scrollResp.getHits().getHits()) {
      Map<String, Object> metadata = hit.getSource();
      Map<String, Object> extractedTermMap = new HashMap<String, Object>();
      try {

        String dataset = (String) metadata.get("Dataset-ShortName");
        String dataAbstract = (String) metadata.get("Dataset-Description");
        String longname = (String) metadata.get("Dataset-LongName");

        if (metadata.containsKey("extractedTerms")) {

          String extractedTerms = (String) metadata.get("extractedTerms");

          // System.out.println(extractedTerms);
          if (!extractedTerms.equals("")) {
            continue;
          }
        }

        String oriShorname = dataset.replaceAll("-", " ").replaceAll("_", " ");
        dataAbstract = es.customAnalyzing(indexName, dataAbstract);
        dataAbstract = dataAbstract.replaceAll("\"", "");
        String oriTxt = longname + " " + dataAbstract + " " + oriShorname;
        String extractedTerm = termExtractor(dataset, oriTxt);

        System.out.println(dataset);
        System.out.println(extractedTerm);

        extractedTermMap.put("extractedTerms", extractedTerm);
        UpdateRequest ur = es.genUpdateRequest(indexName, metadataType,
            hit.getId(), extractedTermMap);

        es.bulkProcessor.add(ur);
      } catch (Exception e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
      }

    }

    /*SearchResponse sr = es.client.prepareSearch(indexName)
        .setTypes(metadataType).setQuery(QueryBuilders.matchAllQuery())
        .setSize(0).addAggregation(AggregationBuilders.terms("Values")
            .field("DatasetParameter-Term").size(0))
        .execute().actionGet();
    Terms dataterms = sr.getAggregations().get("Values");
    
    int sessionCount = 0;
    for (Terms.Bucket entry : dataterms.getBuckets()) {
    
      FilterBuilder filter_search = FilterBuilders.boolFilter().must(
          FilterBuilders.termFilter("DatasetParameter-Term", entry.getKey()));
      QueryBuilder query_search = QueryBuilders
          .filteredQuery(QueryBuilders.matchAllQuery(), filter_search);
    
      SearchResponse scrollResp = es.client
          .prepareSearch(config.get("indexName")).setTypes(this.metadataType)
          .setScroll(new TimeValue(60000)).setQuery(query_search).setSize(100)
          .execute().actionGet();
    
      List<String> abstracts = new ArrayList<String>();
      String abstractStr = "";
      while (true) {
        for (SearchHit hit : scrollResp.getHits().getHits()) {
          Map<String, Object> metadata = hit.getSource();
          Map<String, Object> extractedTermMap = new HashMap<String, Object>();
          try {
    
            String dataset = (String) metadata.get("Dataset-ShortName");
            String dataAbstract = (String) metadata.get("Dataset-Description");
            String longname = (String) metadata.get("Dataset-LongName");
    
            String oriShorname = dataset.replaceAll("-", " ").replaceAll("_",
                " ");
            dataAbstract = es.customAnalyzing(indexName, dataAbstract);
            dataAbstract = dataAbstract.replaceAll("\"", "");
            String oriTxt = longname + " " + dataAbstract + " " + oriShorname;
    
            abstractStr += oriTxt + " ";
            abstracts.add(oriTxt);
          } catch (Exception e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
          }
        }
    
        scrollResp = es.client.prepareSearchScroll(scrollResp.getScrollId())
            .setScroll(new TimeValue(600000)).execute().actionGet();
        if (scrollResp.getHits().getHits().length == 0) {
          break;
        }
    
      }
    
      // String terms = this.termExtractor("test", abstractStr);
      System.out.println(abstractStr);
    }*/

    /* SearchResponse scrollResp = es.client.prepareSearch(indexName)
        .setTypes(metadataType).setScroll(new TimeValue(60000))
        .setQuery(QueryBuilders.matchAllQuery()).setSize(100).execute()
        .actionGet();

    String abstractStr = "";
    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> metadata = hit.getSource();
        Map<String, Object> extractedTermMap = new HashMap<String, Object>();
        try {

          String dataset = (String) metadata.get("Dataset-ShortName");
          String dataAbstract = (String) metadata.get("Dataset-Description");
          String longname = (String) metadata.get("Dataset-LongName");

          String oriShorname = dataset.replaceAll("-", " ").replaceAll("_",
              " ");
          dataAbstract = es.customAnalyzing(indexName, dataAbstract);
          dataAbstract = dataAbstract.replaceAll("\"", "");
          String oriTxt = longname + " " + dataAbstract + " " + oriShorname;
          abstractStr += oriTxt + " ";
          System.out.println(abstractStr);
        } catch (Exception e1) {
          // TODO Auto-generated catch block
          e1.printStackTrace();
        }
      }

      scrollResp = es.client.prepareSearchScroll(scrollResp.getScrollId())
          .setScroll(new TimeValue(600000)).execute().actionGet();
      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }
    }
    System.out.println("test");
    System.out.println(abstractStr);*/
    // es.destroyBulkProcessor();
  }

  public String termExtractor(String dataset, String dataAbstract) {
    HttpClient httpclient = HttpClients.createDefault();
    String terms = "";
    try {

      URIBuilder builder = new URIBuilder(
          "https://westus.api.cognitive.microsoft.com/text/analytics/v2.0/keyPhrases");

      URI uri = builder.build();
      HttpPost request = new HttpPost(uri);
      request.setHeader("Content-Type", "application/json");
      request.setHeader("Ocp-Apim-Subscription-Key",
          "d596e61cc18441699430bff400ea12b0");

      // Request body
      String requestPara = "{\"documents\": [{\"language\": \"en\", \"id\": \""
          + dataset + "\",\"text\":\"" + dataAbstract + "\"}]}";

      // System.out.println(requestPara);
      StringEntity reqEntity = new StringEntity(requestPara);
      request.setEntity(reqEntity);

      HttpResponse response = httpclient.execute(request);
      HttpEntity entity = response.getEntity();

      if (entity != null) {
        String result = EntityUtils.toString(entity);
        System.out.println(result);
        JSONObject jsonObj = new JSONObject(result);
        JSONArray docs = jsonObj.getJSONArray("documents");
        JSONObject keyPhrases = (JSONObject) docs.get(0);
        JSONArray termArr = keyPhrases.getJSONArray("keyPhrases");
        terms = termArr.toString();
        terms = terms.substring(1, terms.length() - 1).replace("\"", "");
        // System.out.println(terms);
      }

    } catch (Exception e) {
      System.out.println(dataset + ":" + e.getMessage());
    }

    return terms;
  }

  public void RemoveTermExtractor(ESDriver es) {

    es.createBulkProcesser();

    SearchResponse scrollResp = es.client.prepareSearch(indexName)
        .setTypes(metadataType).setScroll(new TimeValue(60000))
        .setQuery(QueryBuilders.matchAllQuery()).setSize(100).execute()
        .actionGet();
    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> metadata = hit.getSource();
        Map<String, Object> extractedTermMap = new HashMap<String, Object>();
        try {

          String dataset = (String) metadata.get("Dataset-ShortName");
          String dataAbstract = (String) metadata.get("Dataset-Description");
          String longname = (String) metadata.get("Dataset-LongName");

          if (metadata.containsKey("extractedTerms")) {

            extractedTermMap.put("extractedTerms", "");
            UpdateRequest ur = es.genUpdateRequest(indexName, metadataType,
                hit.getId(), extractedTermMap);
            es.bulkProcessor.add(ur);
          }

        } catch (Exception e1) {
          // TODO Auto-generated catch block
          e1.printStackTrace();
        }
      }

      scrollResp = es.client.prepareSearchScroll(scrollResp.getScrollId())
          .setScroll(new TimeValue(600000)).execute().actionGet();
      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }
    }

    es.destroyBulkProcessor();
  }

}
