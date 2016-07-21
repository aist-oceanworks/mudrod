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
package esiptestbed.mudrod.integration;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LinkageIntegration extends DiscoveryStepAbstract {

  private static final Logger LOG = LoggerFactory.getLogger(LinkageIntegration.class);
  private static final long serialVersionUID = 1L;
  transient List<LinkedTerm> termList = new ArrayList<>();
  DecimalFormat df = new DecimalFormat("#.00");
  private static final String INDEX_NAME = "indexName";
  private static final String WEIGHT = "weight";

  public LinkageIntegration(Map<String, String> config, ESDriver es,
      SparkDriver spark) {
    super(config, es, spark);
  }

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

  @Override
  public Object execute() {
    getIngeratedList("sst", 11);
    return null;
  }

  @Override
  public Object execute(Object o) {
    return null;
  }

  public Map<String, Double> appyMajorRule(String input) {
    termList = new ArrayList<>();
    Map<String, Double> termsMap = new HashMap<>();
    Map<String, Double> sortedMap = new HashMap<>();
    try {
      Map<String, List<LinkedTerm>> map = aggregateRelatedTermsFromAllmodel(
          es.customAnalyzing(config.get(INDEX_NAME), input));

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
      sortedMap = sortMapByValue(termsMap); // terms_map will be empty after
      // this step
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
    return sortedMap;
  }

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

  public JsonObject getIngeratedListInJson(String input, int num) {
    Map<String, Double> sortedMap = appyMajorRule(input);
    int count = 0;
    Map<String, Double> trimmedMap = new HashMap<>();
    for (Entry<String, Double> entry : sortedMap.entrySet()) {
      if (count < 10) {
        trimmedMap.put(entry.getKey(), entry.getValue());
      }
      count++;
    }

    return mapToJson(input, trimmedMap);
  }

  public String getModifiedQuery(String input, int num) {
    Map<String, Double> sortedMap = appyMajorRule(input);
    String output = "(" + input.replace(" ", " AND ") + ")";
    int count = 0;
    for (Entry<String, Double> entry : sortedMap.entrySet()) {
      String item = "(" + entry.getKey().replace(" ", " AND ") + ")";
      if (count < num) {
        output += " OR " + item;
      }
      count++;
    }
    return output;
  }

  public Map<String, List<LinkedTerm>> aggregateRelatedTermsFromAllmodel(
      String input) {
    aggregateRelatedTerms(input, config.get("userHistoryLinkageType"));
    aggregateRelatedTerms(input, config.get("clickStreamLinkageType"));
    aggregateRelatedTerms(input, config.get("metadataLinkageType"));
    aggregateRelatedTermsSWEET(input, config.get("ontologyLinkageType"));

    return termList.stream().collect(Collectors.groupingBy(w -> w.term));
  }

  public int getModelweight(String model) {
    if (model.equals(config.get("userHistoryLinkageType"))) {
      return Integer.parseInt(config.get("userHistory_w"));
    }

    if (model.equals(config.get("clickStreamLinkageType"))) {
      return Integer.parseInt(config.get("clickStream_w"));
    }

    if (model.equals(config.get("metadataLinkageType"))) {
      return Integer.parseInt(config.get("metadata_w"));
    }

    if (model.equals(config.get("ontologyLinkageType"))) {
      return Integer.parseInt(config.get("ontology_w"));
    }

    return 999999;
  }

  public String extractRelated(String str, String input) {
    String[] strList = str.split(",");
    if (input.equals(strList[0])) {
      return strList[1];
    } else {
      return strList[0];
    }
  }

  public void aggregateRelatedTerms(String input, String model) {
    SearchResponse usrhis = es.client.prepareSearch(config.get(INDEX_NAME))
        .setTypes(model).setQuery(QueryBuilders.termQuery("keywords", input))
        .addSort(WEIGHT, SortOrder.DESC).setSize(11).execute().actionGet();

    LOG.info("\n************************ {} results***************************", model);
    for (SearchHit hit : usrhis.getHits().getHits()) {
      Map<String, Object> result = hit.getSource();
      String keywords = (String) result.get("keywords");
      String relatedKey = extractRelated(keywords, input);

      if (!relatedKey.equals(input)) {
        LinkedTerm lTerm = new LinkedTerm(relatedKey,
            (double) result.get(WEIGHT), model);
        LOG.info("( {} {} )", relatedKey, (double) result.get(WEIGHT));
        termList.add(lTerm);
      }

    }
  }

  public void aggregateRelatedTermsSWEET(String input, String model) {
    SearchResponse usrhis = es.client.prepareSearch(config.get(INDEX_NAME))
        .setTypes(model).setQuery(QueryBuilders.termQuery("concept_A", input))
        .addSort(WEIGHT, SortOrder.DESC).setSize(11).execute().actionGet();
    LOG.info("\n************************ {} results***************************", model);
    for (SearchHit hit : usrhis.getHits().getHits()) {
      Map<String, Object> result = hit.getSource();
      String conceptB = (String) result.get("concept_B");
      if (!conceptB.equals(input)) {
        LinkedTerm lTerm = new LinkedTerm(conceptB,
            (double) result.get(WEIGHT), model);
        LOG.info("( {} {} )", conceptB, (double) result.get(WEIGHT));
        termList.add(lTerm);
      }
    }
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

  private JsonObject mapToJson(String word, Map<String, Double> wordweights) {
    Gson gson = new Gson();
    JsonObject json = new JsonObject();

    List<JsonObject> nodes = new ArrayList<>();
    JsonObject firstNode = new JsonObject();
    firstNode.addProperty("name", word);
    firstNode.addProperty("group", 1);
    nodes.add(firstNode);
    Set<String> words = wordweights.keySet();
    for (String wordB : words) {
      JsonObject node = new JsonObject();
      node.addProperty("name", wordB);
      node.addProperty("group", 10);
      nodes.add(node);
    }
    JsonElement nodesElement = gson.toJsonTree(nodes);
    json.add("nodes", nodesElement);

    List<JsonObject> links = new ArrayList<>();

    Collection<Double> weights = wordweights.values();
    int num = 1;
    for (double weight : weights) {
      JsonObject link = new JsonObject();
      link.addProperty("source", num);
      link.addProperty("target", 0);
      link.addProperty("value", weight);
      links.add(link);
      num += 1;
    }
    JsonElement linksElement = gson.toJsonTree(links);
    json.add("links", linksElement);

    return json;
  }

}
