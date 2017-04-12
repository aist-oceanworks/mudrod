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
package gov.nasa.jpl.mudrod.integration;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import gov.nasa.jpl.mudrod.discoveryengine.DiscoveryStepAbstract;
import gov.nasa.jpl.mudrod.driver.ESDriver;
import gov.nasa.jpl.mudrod.driver.SparkDriver;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Supports ability to integrate vocab similarity results from metadata, ontology, and web logs.
 */
public class LinkageIntegration extends DiscoveryStepAbstract {

  private static final Logger LOG = LoggerFactory.getLogger(LinkageIntegration.class);
  private static final long serialVersionUID = 1L;
  transient List<LinkedTerm> termList = new ArrayList<>();
  DecimalFormat df = new DecimalFormat("#.00");
  private static final String INDEX_NAME = "indexName";
  private static final String WEIGHT = "weight";

  public LinkageIntegration(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  /**
   * The data structure to store semantic triple.
   */
  class LinkedTerm {
    String term = null;
    double weight = 0;
    String model = null;

    public LinkedTerm(String str, double w, String m) {
      term = str;
      weight = w;
      model = m;
    }
  }

  /**
   * Method of executing integration step
   */
  @Override
  public Object execute() {
    getIngeratedList("ocean wind", 11);
    return null;
  }

  @Override
  public Object execute(Object o) {
    return null;
  }

  /**
   * Method of getting integrated results
   *
   * @param input query string
   * @return a hash map where the string is a related term, and double is the
   * similarity to the input query
   */
  public Map<String, Double> appyMajorRule(String input) {
    termList = new ArrayList<>();
    Map<String, Double> termsMap = new HashMap<>();
    Map<String, List<LinkedTerm>> map = new HashMap<>();
    try {
      map = aggregateRelatedTermsFromAllmodel(es.customAnalyzing(props.getProperty(INDEX_NAME), input));
    } catch (InterruptedException | ExecutionException e) {
      LOG.error("Error applying majority rule", e);
    }

    for (Entry<String, List<LinkedTerm>> entry : map.entrySet()) {
      List<LinkedTerm> list = entry.getValue();
      double sumModelWeight = 0;
      double tmp = 0;
      for (LinkedTerm element : list) {
        sumModelWeight += getModelweight(element.model);

        if (element.weight > tmp) {
          tmp = element.weight;
        }
      }

      double finalWeight = tmp + ((sumModelWeight - 2) * 0.05);
      if (finalWeight < 0) {
        finalWeight = 0;
      }

      if (finalWeight > 1) {
        finalWeight = 1;
      }
      termsMap.put(entry.getKey(), Double.parseDouble(df.format(finalWeight)));
    }

    return sortMapByValue(termsMap);
  }

  /**
   * Method of getting integrated results
   *
   * @param input query string
   * @param num   the number of most related terms
   * @return a string of related terms along with corresponding similarities
   */
  public String getIngeratedList(String input, int num) {
    String output = "";
    Map<String, Double> sortedMap = appyMajorRule(input);
    int count = 0;
    for (Entry<String, Double> entry : sortedMap.entrySet()) {
      if (count < num) {
        output += entry.getKey() + " = " + entry.getValue() + ", ";
      }
      count++;
    }
    LOG.info("\n************************Integrated results***************************");
    LOG.info(output);
    return output;
  }

  /**
   * Method of getting integrated results
   *
   * @param input query string
   * @return a JSON object of related terms along with corresponding similarities
   */
  public JsonObject getIngeratedListInJson(String input) {
    Map<String, Double> sortedMap = appyMajorRule(input);
    int count = 0;
    Map<String, Double> trimmedMap = new LinkedHashMap<>();
    for (Entry<String, Double> entry : sortedMap.entrySet()) {
      if (!entry.getKey().contains("china")) {
        if (count < 10) {
          trimmedMap.put(entry.getKey(), entry.getValue());
        }
        count++;
      }
    }

    return mapToJson(trimmedMap);
  }

  /**
   * Method of aggregating terms from web logs, metadata, and ontology
   *
   * @param input query string
   * @return a hash map where the string is a related term, and the list is
   * the similarities from different sources
   */
  public Map<String, List<LinkedTerm>> aggregateRelatedTermsFromAllmodel(String input) {
    aggregateRelatedTerms(input, props.getProperty("userHistoryLinkageType"));
    aggregateRelatedTerms(input, props.getProperty("clickStreamLinkageType"));
    aggregateRelatedTerms(input, props.getProperty("metadataLinkageType"));
    aggregateRelatedTermsSWEET(input, props.getProperty("ontologyLinkageType"));

    return termList.stream().collect(Collectors.groupingBy(w -> w.term));
  }

  public int getModelweight(String model) {
    if (model.equals(props.getProperty("userHistoryLinkageType"))) {
      return Integer.parseInt(props.getProperty("userHistory_w"));
    }

    if (model.equals(props.getProperty("clickStreamLinkageType"))) {
      return Integer.parseInt(props.getProperty("clickStream_w"));
    }

    if (model.equals(props.getProperty("metadataLinkageType"))) {
      return Integer.parseInt(props.getProperty("metadata_w"));
    }

    if (model.equals(props.getProperty("ontologyLinkageType"))) {
      return Integer.parseInt(props.getProperty("ontology_w"));
    }

    return 999999;
  }

  /**
   * Method of extracting the related term from a comma string
   *
   * @param str   input string
   * @param input query string
   * @return related term contained in the input string
   */
  public String extractRelated(String str, String input) {
    String[] strList = str.split(",");
    if (input.equals(strList[0])) {
      return strList[1];
    } else {
      return strList[0];
    }
  }

  public void aggregateRelatedTerms(String input, String model) {
    //get the first 10 related terms
    SearchResponse usrhis = es.getClient().prepareSearch(props.getProperty(INDEX_NAME)).setTypes(model).setQuery(QueryBuilders.termQuery("keywords", input)).addSort(WEIGHT, SortOrder.DESC).setSize(11)
        .execute().actionGet();

    LOG.info("\n************************ {} results***************************", model);
    for (SearchHit hit : usrhis.getHits().getHits()) {
      Map<String, Object> result = hit.getSource();
      String keywords = (String) result.get("keywords");
      String relatedKey = extractRelated(keywords, input);

      if (!relatedKey.equals(input)) {
        LinkedTerm lTerm = new LinkedTerm(relatedKey, (double) result.get(WEIGHT), model);
        LOG.info("( {} {} )", relatedKey, (double) result.get(WEIGHT));
        termList.add(lTerm);
      }

    }
  }

  /**
   * Method of querying related terms from ontology
   *
   * @param input input query
   * @param model source name
   */
  public void aggregateRelatedTermsSWEET(String input, String model) {
    SearchResponse usrhis = es.getClient().prepareSearch(props.getProperty(INDEX_NAME)).setTypes(model).setQuery(QueryBuilders.termQuery("concept_A", input)).addSort(WEIGHT, SortOrder.DESC)
        .setSize(11).execute().actionGet();
    LOG.info("\n************************ {} results***************************", model);
    for (SearchHit hit : usrhis.getHits().getHits()) {
      Map<String, Object> result = hit.getSource();
      String conceptB = (String) result.get("concept_B");
      if (!conceptB.equals(input)) {
        LinkedTerm lTerm = new LinkedTerm(conceptB, (double) result.get(WEIGHT), model);
        LOG.info("( {} {} )", conceptB, (double) result.get(WEIGHT));
        termList.add(lTerm);
      }
    }
  }

  /**
   * Method of sorting a map by value
   *
   * @param passedMap input map
   * @return sorted map
   */
  public Map<String, Double> sortMapByValue(Map<String, Double> passedMap) {
    List<String> mapKeys = new ArrayList<>(passedMap.keySet());
    List<Double> mapValues = new ArrayList<>(passedMap.values());
    Collections.sort(mapValues, Collections.reverseOrder());
    Collections.sort(mapKeys, Collections.reverseOrder());

    LinkedHashMap<String, Double> sortedMap = new LinkedHashMap<>();

    Iterator<Double> valueIt = mapValues.iterator();
    while (valueIt.hasNext()) {
      Object val = valueIt.next();
      Iterator<String> keyIt = mapKeys.iterator();

      while (keyIt.hasNext()) {
        Object key = keyIt.next();
        String comp1 = passedMap.get(key).toString();
        String comp2 = val.toString();

        if (comp1.equals(comp2)) {
          passedMap.remove(key);
          mapKeys.remove(key);
          sortedMap.put((String) key, (Double) val);
          break;
        }

      }

    }
    return sortedMap;
  }

  /**
   * Method of converting hashmap to JSON
   *
   * @param word        input query
   * @param wordweights a map from related terms to weights
   * @return converted JSON object
   */
  private JsonObject mapToJson(Map<String, Double> wordweights) {
    Gson gson = new Gson();
    JsonObject json = new JsonObject();
    List<JsonObject> nodes = new ArrayList<>();

    for (Entry<String, Double> entry : wordweights.entrySet()) {
      JsonObject node = new JsonObject();
      String key = entry.getKey();
      Double value = entry.getValue();
      node.addProperty("word", key);
      node.addProperty("weight", value);
      nodes.add(node);
    }

    JsonElement nodesElement = gson.toJsonTree(nodes);
    json.add("ontology", nodesElement);

    return json;
  }

}
