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
package gov.nasa.jpl.mudrod.recommendation.structure;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import gov.nasa.jpl.mudrod.discoveryengine.DiscoveryStepAbstract;
import gov.nasa.jpl.mudrod.driver.ESDriver;
import gov.nasa.jpl.mudrod.driver.SparkDriver;
import gov.nasa.jpl.mudrod.main.MudrodEngine;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

/**
 * Recommend metadata using combination all two methods, including content-based
 * similarity and session-level similarity
 */
public class HybridRecommendation extends DiscoveryStepAbstract {
  /**
   *
   */
  private static final long serialVersionUID = 1L;
  // recommended metadata list
  protected transient List<LinkedTerm> termList = new ArrayList<>();
  // format decimal
  DecimalFormat df = new DecimalFormat("#.00");
  // index name
  protected static final String INDEX_NAME = "indexName";
  private static final String WEIGHT = "weight";

  /**
   * recommended data class Date: Sep 12, 2016 2:25:28 AM
   */
  class LinkedTerm {
    public String term = null;
    public double weight = 0;
    public String model = null;

    public LinkedTerm(String str, double w, String m) {
      term = str;
      weight = w;
      model = m;
    }
  }

  public HybridRecommendation(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  @Override
  public Object execute() {
    return null;
  }

  @Override
  public Object execute(Object o) {
    return null;
  }

  /**
   * Get recommended data for a giving dataset
   *
   * @param input: a giving dataset
   * @param num:   the number of recommended dataset
   * @return recommended dataset in json format
   */
  public JsonObject getRecomDataInJson(String input, int num) {
    JsonObject resultJson = new JsonObject();

    String type = props.getProperty("metadataCodeSimType");
    Map<String, Double> sortedVariableSimMap = getRelatedData(type, input, num + 10);

    type = props.getProperty("metadataWordTFIDFSimType");
    Map<String, Double> sortedAbstractSimMap = getRelatedData(type, input, num + 10);

    type = props.getProperty("metadataSessionBasedSimType");
    Map<String, Double> sortedSessionSimMap = getRelatedData(type, input, num + 10);

    JsonElement variableSimJson = mapToJson(sortedVariableSimMap, num);
    resultJson.add("variableSim", variableSimJson);
    JsonElement abstractSimJson = mapToJson(sortedAbstractSimMap, num);
    resultJson.add("abstractSim", abstractSimJson);
    JsonElement sessionSimJson = mapToJson(sortedSessionSimMap, num);
    resultJson.add("sessionSim", sessionSimJson);

    Map<String, Double> hybirdSimMap = new HashMap<String, Double>();

    for (String name : sortedAbstractSimMap.keySet()) {
      hybirdSimMap.put(name, sortedAbstractSimMap.get(name) /** 0.4 */);
    }

    for (String name : sortedVariableSimMap.keySet()) {
      if (hybirdSimMap.get(name) != null) {
        double sim = hybirdSimMap.get(name) + sortedVariableSimMap.get(name) /** 0.3 */;
        hybirdSimMap.put(name, Double.parseDouble(df.format(sim)));
      } else {
        double sim = sortedVariableSimMap.get(name);
        hybirdSimMap.put(name, Double.parseDouble(df.format(sim)));
      }
    }

    for (String name : sortedSessionSimMap.keySet()) {
      if (hybirdSimMap.get(name) != null) {
        double sim = hybirdSimMap.get(name) + sortedSessionSimMap.get(name) /** 0.1 */;
        hybirdSimMap.put(name, Double.parseDouble(df.format(sim)));
      } else {
        double sim = sortedSessionSimMap.get(name);
        hybirdSimMap.put(name, Double.parseDouble(df.format(sim)));
      }
    }

    Map<String, Double> sortedHybirdSimMap = this.sortMapByValue(hybirdSimMap);

    JsonElement linkedJson = mapToJson(sortedHybirdSimMap, num);
    resultJson.add("linked", linkedJson);

    return resultJson;
  }

  /**
   * Method of converting hashmap to JSON
   *
   * @param wordweights a map from related metadata to weights
   * @param num         the number of converted elements
   * @return converted JSON object
   */
  protected JsonElement mapToJson(Map<String, Double> wordweights, int num) {
    Gson gson = new Gson();

    List<JsonObject> nodes = new ArrayList<>();
    Set<String> words = wordweights.keySet();
    int i = 0;
    for (String wordB : words) {
      JsonObject node = new JsonObject();
      node.addProperty("name", wordB);
      node.addProperty("weight", wordweights.get(wordB));
      nodes.add(node);

      i += 1;
      if (i >= num) {
        break;
      }
    }

    String nodesJson = gson.toJson(nodes);
    JsonElement nodesElement = gson.fromJson(nodesJson, JsonElement.class);

    return nodesElement;
  }

  /**
   * Get recommend dataset for a giving dataset
   *
   * @param type  recommend method
   * @param input a giving dataset
   * @param num   the number of recommended dataset
   * @return recommended dataset map, key is dataset name, value is similarity
   * value
   */
  public Map<String, Double> getRelatedData(String type, String input, int num) {
    termList = new ArrayList<>();
    Map<String, Double> termsMap = new HashMap<>();
    Map<String, Double> sortedMap = new HashMap<>();
    try {
      List<LinkedTerm> links = getRelatedDataFromES(type, input, num);
      int size = links.size();
      for (int i = 0; i < size; i++) {
        termsMap.put(links.get(i).term, links.get(i).weight);
      }

      sortedMap = sortMapByValue(termsMap); // terms_map will be empty
    } catch (Exception e) {
      e.printStackTrace();
    }

    return sortedMap;
  }

  /**
   * Get recommend dataset for a giving dataset
   *
   * @param type  recommend method
   * @param input a giving dataset
   * @param num   the number of recommended dataset
   * @return recommended dataset list
   */
  public List<LinkedTerm> getRelatedDataFromES(String type, String input, int num) {

    SearchRequestBuilder builder = es.getClient().prepareSearch(props.getProperty(INDEX_NAME)).setTypes(type).setQuery(QueryBuilders.termQuery("concept_A", input)).addSort(WEIGHT, SortOrder.DESC)
        .setSize(num);

    SearchResponse usrhis = builder.execute().actionGet();

    for (SearchHit hit : usrhis.getHits().getHits()) {
      Map<String, Object> result = hit.getSource();
      String conceptB = (String) result.get("concept_B");

      if (!conceptB.equals(input)) {
        LinkedTerm lTerm = new LinkedTerm(conceptB, (double) result.get(WEIGHT), type);
        termList.add(lTerm);
      }
    }

    return termList;
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

  public static void main(String[] args) throws IOException {

    MudrodEngine me = new MudrodEngine();
    Properties props = me.loadConfig();
    ESDriver es = new ESDriver(me.getConfig());
    HybridRecommendation test = new HybridRecommendation(props, es, null);

    // String input = "NSCAT_LEVEL_1.7_V2";
    String input = "AQUARIUS_L3_SSS_SMIA_MONTHLY-CLIMATOLOGY_V4";
    JsonObject json = test.getRecomDataInJson(input, 10);

    System.out.println(json.toString());
  }
}
