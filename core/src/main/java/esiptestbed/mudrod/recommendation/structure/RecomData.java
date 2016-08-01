package esiptestbed.mudrod.recommendation.structure;

import java.text.DecimalFormat;
import java.util.ArrayList;
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

import org.apache.commons.collections.map.LinkedMap;
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


public class RecomData extends DiscoveryStepAbstract {

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

	public RecomData(Map<String, String> config, ESDriver es, SparkDriver spark) {
		super(config, es, spark);
		// TODO Auto-generated constructor stub
	}

	@Override
	public Object execute() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object execute(Object o) {
		// TODO Auto-generated method stub
		return null;
	}

	public JsonObject getRecomDataInJson(String input, int num) {
		Map<String, Double> sortedMap = getRelatedData(input);
		//System.out.println(sortedMap);
		int count = 0;
		Map<String, Double> trimmedMap = new LinkedMap();
		for (Entry<String, Double> entry : sortedMap.entrySet()) {
			if (count < num) {
				trimmedMap.put(entry.getKey(), entry.getValue());
			}
			count++;
		}

		//System.out.println(trimmedMap);
		
		return mapToJson(input, trimmedMap);
	}
	
	public Map<String, Double> getRelatedData(String input) {
		termList = new ArrayList<>();
		Map<String, Double> termsMap = new HashMap<>();
		Map<String, Double> sortedMap = new HashMap<>();
		try {
			Map<String, List<LinkedTerm>> map = aggregateRelatedTermsFromAllmodel(input);
			for (Entry<String, List<LinkedTerm>> entry : map.entrySet()) {
				List<LinkedTerm> list = entry.getValue();
				double finalWeight = 0;
				for (LinkedTerm element : list) {
					finalWeight += element.weight;
				}

				termsMap.put(entry.getKey(), finalWeight);
			}

			sortedMap = sortMapByValue(termsMap); // terms_map will be empty
		} catch (Exception e) {
			e.printStackTrace();
		}
		return sortedMap;
	}

	protected JsonObject mapToJson(String word, Map<String, Double> wordweights) {
		Gson gson = new Gson();
		JsonObject json = new JsonObject();

		List<JsonObject> nodes = new ArrayList<>();
		Set<String> words = wordweights.keySet();
		for (String wordB : words) {
			JsonObject node = new JsonObject();
			node.addProperty("name", wordB);
			node.addProperty("weight", wordweights.get(wordB));
			nodes.add(node);
		}
		String nodesJson = gson.toJson(nodes);
		JsonElement nodesElement = gson.fromJson(nodesJson,JsonElement.class);
		json.add("related", nodesElement);

		return json;
	}

	public Map<String, List<LinkedTerm>> aggregateRelatedTermsFromAllmodel(String input) {

		String customInput = null;
		try {
			customInput = es.customAnalyzing(config.get(INDEX_NAME), input);
		} catch (InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String type = config.get("metadataCodeSimType");
		SearchResponse usrhis = es.client.prepareSearch(config.get(INDEX_NAME)).setTypes(type)
				.setQuery(QueryBuilders.termQuery("keywords", customInput)).addSort(WEIGHT, SortOrder.DESC).setSize(11)
				.execute().actionGet();

		for (SearchHit hit : usrhis.getHits().getHits()) {
			Map<String, Object> result = hit.getSource();
			String keywords = (String) result.get("keywords");
			String relatedKey = extractRelated(keywords, input);

			if (!relatedKey.equals(input)) {
				LinkedTerm lTerm = new LinkedTerm(relatedKey, (double) result.get(WEIGHT), type);

				//System.out.println(input + " : " + relatedKey + " : " + (double) result.get(WEIGHT));
				termList.add(lTerm);
			}
		}

		return termList.stream().collect(Collectors.groupingBy(w -> w.term));
	}

	private String extractRelated(String str, String input) {
		String[] strList = str.split(",");
		if (input.equals(strList[0])) {
			return strList[1];
		} else {
			return strList[0];
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
}
