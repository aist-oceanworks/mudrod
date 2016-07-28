package esiptestbed.mudrod.ssearch;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import esiptestbed.mudrod.discoveryengine.MudrodAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.integration.LinkageIntegration;
import esiptestbed.mudrod.main.MudrodEngine;

public class Dispatcher extends MudrodAbstract {

  public Dispatcher(Map<String, String> config, ESDriver es, SparkDriver spark) {
    super(config, es, spark);
    // TODO Auto-generated constructor stub
    if(es.checkTypeExist(config.get("indexName"), config.get("clickstreamMatrixType")))
    {
      ClickstreamImporter cs = new ClickstreamImporter(config, es, spark);
      cs.importfromCSVtoES();
    }
  }

  public Map<String, Double> getRelatedTerms(String input, int num) {
    LinkageIntegration li = new LinkageIntegration(this.config,
        this.es, null);
    Map<String, Double> sortedMap = li.appyMajorRule(input);
    Map<String, Double> selected_Map = new HashMap<>();
    int count = 0;
    for (Entry<String, Double> entry : sortedMap.entrySet()) {
      if (count < num) {
        selected_Map.put(entry.getKey(), entry.getValue());
      }
      count++;
    }
    return selected_Map;
  }

  public BoolQueryBuilder createSemQuery(String input, int num){
    Map<String, Double> selected_Map = getRelatedTerms(input, num);
    selected_Map.put(input, (double) 1);

    String fieldsList[] = {"Dataset-Metadata", "Dataset-ShortName", "Dataset-LongName", "Dataset-Description", "DatasetParameter-*"};

    BoolQueryBuilder qb = new BoolQueryBuilder();
    for (Entry<String, Double> entry : selected_Map.entrySet()){
      qb.should(QueryBuilders.multiMatchQuery(entry.getKey(), fieldsList)
          .boost(entry.getValue().floatValue())
          .type(MultiMatchQueryBuilder.Type.PHRASE));
    }
    return qb;
  }
  
  public QueryBuilder createQueryForClicks(Map<String, Double> selected_Map, String shortName){   
    BoolFilterBuilder bf = new BoolFilterBuilder();
    bf.must(FilterBuilders.termFilter("dataID", shortName));
    
    for (Map.Entry<String, Double> entry : selected_Map.entrySet()){
      bf.should(FilterBuilders.termFilter("query", entry.getKey()));      
    }
    QueryBuilder click_search = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), bf);
    
    return click_search;
  }
  
  public static void main(String[] args) {
    MudrodEngine mudrod = new MudrodEngine();
    Dispatcher dp = new Dispatcher(mudrod.getConfig(), mudrod.getES(), null);
    dp.createSemQuery("ocean wind", 2);
  }

}
