package esiptestbed.mudrod.kgraph;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;

import esiptestbed.mudrod.discoveryengine.MudrodAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.metadata.structure.Metadata;
import esiptestbed.mudrod.ssearch.Dispatcher;

public class NERer extends MudrodAbstract {

	public NERer(Properties props, ESDriver es, SparkDriver spark) {
		super(props, es, spark);
		// TODO Auto-generated constructor stub
	}
	
	private static final Map<String, String> NERMap = createMap();
    private static Map<String, String> createMap()
    {
        Map<String,String> map = new HashMap<String,String>();
        map.put("DatasetParameter-Variable", "Variable");
        map.put("DatasetParameter-Topic", "Topic");
        map.put("DatasetParameter-Term", "Term");
        map.put("DatasetProject-Project-ShortName", "Project(shortName)");
        map.put("DatasetProject-Project-LongName", "Project(longName)");
        map.put("DatasetSource-Source-LongName", "Source(longName)");
        map.put("DatasetSource-Source-ShortName", "Source(shortName)");
        map.put("DatasetSource-Source-Type", "SourceType");
        map.put("DatasetSource-Sensor-LongName", "Sensor(longName)");
        map.put("DatasetSource-Sensor-ShortName", "Sensor(shortName)");
        map.put("Collection-LongName", "Collection(longName)");
        map.put("Collection-ShortName", "Collection(shortName)");
        map.put("DatasetParameter-VariableDetail", "VariableDetail");
        map.put("DatasetParameter-Category", "Category");
        return map;
    }

	public String getNER(String term) {
		Dispatcher dp = new Dispatcher(this.getConfig(), this.getES(), null);
		for (int m = 0; m < Metadata.fieldsList.length; m++) {
			if (!Metadata.fieldsList[m].equals("Dataset-Metadata")) {
				BoolQueryBuilder qb = dp.getQueryForField(term, Metadata.fieldsList[m]);
				SearchResponse response = es.getClient().prepareSearch(props.getProperty("indexName"))
						.setTypes("ner").setQuery(qb).setSize(0).execute().actionGet();
				long count = response.getHits().totalHits();

				if (count > 0)
					return "hasRelated" + NERMap.get(Metadata.fieldsList[m]);
			}
		}
		return "unknown";
	}
	
	

}
