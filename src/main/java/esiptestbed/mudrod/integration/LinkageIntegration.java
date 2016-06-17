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

public class LinkageIntegration extends DiscoveryStepAbstract {
  List<LinkedTerm> termList = new ArrayList<LinkedTerm>();
  DecimalFormat df = new DecimalFormat("#.00");

  public LinkageIntegration(Map<String, String> config, ESDriver es,
      SparkDriver spark) {
    super(config, es, spark);
    // TODO Auto-generated constructor stub
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
    // TODO Auto-generated method stub
    getIngeratedList("sst", 11);
    return null;
  }

  @Override
  public Object execute(Object o) {
    // TODO Auto-generated method stub
    return null;
  }

  public Map<String, Double> appyMajorRule(String input) {
    termList = new ArrayList<LinkedTerm>();
    Map<String, Double> terms_map = new HashMap<String, Double>();
    Map<String, Double> sortedMap = new HashMap<String, Double>();
    try {
      Map<String, List<LinkedTerm>> map = aggregateRelatedTermsFromAllmodel(
          es.customAnalyzing(config.get("indexName"), input));

      for (Entry<String, List<LinkedTerm>> entry : map.entrySet()) {
        List<LinkedTerm> list = entry.getValue();
        double sum_model_w = 0;
        double tmp = 0;
        for (LinkedTerm element : list) {
          sum_model_w += getModelweight(element.model);

          if (element.weight > tmp) {
            tmp = element.weight;
          }
        }

        double final_w = tmp + ((sum_model_w - 2) * 0.05);
        if (final_w < 0) {
          final_w = 0;
        }

        if (final_w > 1) {
          final_w = 1;
        }
        terms_map.put(entry.getKey(), Double.parseDouble(df.format(final_w)));
      }
      sortedMap = sortMapByValue(terms_map); // terms_map will be empty after
                                             // this step
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (ExecutionException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return sortedMap;
  }

  public String getIngeratedList(String input, int Num) {
    String output = "";
    Map<String, Double> sortedMap = appyMajorRule(input);
    int count = 0;
    for (Entry<String, Double> entry : sortedMap.entrySet()) {
      if (count < Num) {
        output += entry.getKey() + " = " + entry.getValue() + ", ";
      }
      count++;
    }
    System.out.println(
        "\n************************Integrated results***************************");
    System.out.println(output);
    return output;
  }

  public JsonObject getIngeratedListInJson(String input, int Num) {
    Map<String, Double> sortedMap = appyMajorRule(input);
    int count = 0;
    Map<String, Double> trimmed_map = new HashMap<String, Double>();
    for (Entry<String, Double> entry : sortedMap.entrySet()) {
      if (count < 10) {
        trimmed_map.put(entry.getKey(), entry.getValue());
      }
      count++;
    }

    return MapToJson(input, trimmed_map);
  }

  public String getModifiedQuery(String input, int Num) {
    Map<String, Double> sortedMap = appyMajorRule(input);
    String output = "(" + input.replace(" ", " AND ") + ")";
    int count = 0;
    for (Entry<String, Double> entry : sortedMap.entrySet()) {
      String item = "(" + entry.getKey().replace(" ", " AND ") + ")";
      if (count < Num) {
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
    String[] str_List = str.split(",");
    if (input.equals(str_List[0])) {
      return str_List[1];
    } else {
      return str_List[0];
    }
  }

  public void aggregateRelatedTerms(String input, String model) {
    SearchResponse usrhis = es.client.prepareSearch(config.get("indexName"))
        .setTypes(model).setQuery(QueryBuilders.termQuery("keywords", input))
        .addSort("weight", SortOrder.DESC).setSize(11).execute().actionGet();

    System.out.println("\n************************" + model
        + " results***************************");
    for (SearchHit hit : usrhis.getHits().getHits()) {
      Map<String, Object> result = hit.getSource();
      String keywords = (String) result.get("keywords");
      String relatedKey = extractRelated(keywords, input);

      if (!relatedKey.equals(input)) {
        LinkedTerm lTerm = new LinkedTerm(relatedKey,
            (double) result.get("weight"), model);

        System.out
            .print(relatedKey + "(" + (double) result.get("weight") + "), ");
        termList.add(lTerm);
      }

    }
  }

  public void aggregateRelatedTermsSWEET(String input, String model) {
    SearchResponse usrhis = es.client.prepareSearch(config.get("indexName"))
        .setTypes(model).setQuery(QueryBuilders.termQuery("concept_A", input))
        .addSort("weight", SortOrder.DESC).setSize(11).execute().actionGet();
    System.out.println("\n************************" + model
        + " results***************************");
    for (SearchHit hit : usrhis.getHits().getHits()) {
      Map<String, Object> result = hit.getSource();
      String concept_B = (String) result.get("concept_B");
      if (!concept_B.equals(input)) {
        LinkedTerm lTerm = new LinkedTerm(concept_B,
            (double) result.get("weight"), model);
        System.out
            .print(concept_B + "(" + (double) result.get("weight") + "), ");
        termList.add(lTerm);
      }
    }
  }

  public LinkedHashMap<String, Double> sortMapByValue(Map passedMap) {
    List mapKeys = new ArrayList(passedMap.keySet());
    List mapValues = new ArrayList(passedMap.values());
    Collections.sort(mapValues, Collections.reverseOrder());
    Collections.sort(mapKeys, Collections.reverseOrder());

    LinkedHashMap sortedMap = new LinkedHashMap();

    Iterator valueIt = mapValues.iterator();
    while (valueIt.hasNext()) {
      Object val = valueIt.next();
      Iterator keyIt = mapKeys.iterator();

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

  private JsonObject MapToJson(String word, Map<String, Double> wordweights) {
    Gson gson = new Gson();
    JsonObject json = new JsonObject();

    List<JsonObject> nodes = new ArrayList<JsonObject>();
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

    List<JsonObject> links = new ArrayList<JsonObject>();

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
