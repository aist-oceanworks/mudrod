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
package esiptestbed.mudrod.recommendation.structure;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;

/**
 * This class is used to test recommendation result similarity and session-level
 * similarity 
 */
public class RecommendationData extends DiscoveryStepAbstract {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  protected transient List<LinkedTerm> termList = new ArrayList<>();
  DecimalFormat df = new DecimalFormat("#.00");
  protected static final String INDEX_NAME = "indexName";
  private static final String WEIGHT = "weight";

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

  public RecommendationData(Properties props, ESDriver es, SparkDriver spark) {
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

  public JsonObject getRecomDataInJson(String input, int num) {
    String type = props.getProperty("metadataSessionBasedSimType");
    Map<String, Double> sortedOBSimMap = getRelatedData(type, input, num + 5);
    JsonElement linkedJson = mapToJson(sortedOBSimMap, num);

    type = props.getProperty("metadataTopicSimType");

    Map<String, Double> sortedMBSimMap = getRelatedData(type, input, num + 5);
    JsonElement relatedJson = mapToJson(sortedMBSimMap, num);

    JsonObject json = new JsonObject();

    json.add("linked", linkedJson);
    json.add("related", relatedJson);

    return json;
  }

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
    return gson.fromJson(gson.toJson(nodes), JsonElement.class);
  }

  public Map<String, Double> getRelatedData(String type, String input,
      int num) {
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

  public List<LinkedTerm> getRelatedDataFromES(String type, String input,
      int num) {
    SearchRequestBuilder builder = es.getClient()
        .prepareSearch(props.getProperty(INDEX_NAME)).setTypes(type)
        .setQuery(QueryBuilders.termQuery("concept_A", input))
        .addSort(WEIGHT, SortOrder.DESC).setSize(num);

    SearchResponse usrhis = builder.execute().actionGet();

    for (SearchHit hit : usrhis.getHits().getHits()) {
      Map<String, Object> result = hit.getSource();
      String conceptB = (String) result.get("concept_B");

      if (!conceptB.equals(input)) {
        LinkedTerm lTerm = new LinkedTerm(conceptB, (double) result.get(WEIGHT),
            type);
        termList.add(lTerm);
      }
    }

    return termList;
  }

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
}
