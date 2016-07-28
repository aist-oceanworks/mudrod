/**
 * 
 */
package esiptestbed.mudrod.ssearch;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.index.query.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import esiptestbed.mudrod.discoveryengine.MudrodAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.main.MudrodEngine;
import esiptestbed.mudrod.ssearch.structure.SResult;

/**
 * @author Yongyao
 *
 */
public class Searcher extends MudrodAbstract {

  private static final Logger LOG = LoggerFactory.getLogger(ESDriver.class);
  /**
   * @param config
   * @param es
   * @param spark
   */
  public Searcher(Map<String, String> config, ESDriver es, SparkDriver spark) {
    super(config, es, spark);
    // TODO Auto-generated constructor stub
  }


  public List<SResult> searchByQuery(String index, String type, String query) 
  {
    boolean exists = es.node.client().admin().indices().prepareExists(index).execute().actionGet().isExists();	
    if(!exists){
      return null;
    }
    
    Dispatcher dp = new Dispatcher(this.getConfig(), this.getES(), null);   
    BoolQueryBuilder qb = dp.createSemQuery(query, 2);
    Map<String, Double> selected_Map = dp.getRelatedTerms(query, 2);
    List<SResult> resultList = new ArrayList<SResult>();
    
    SearchResponse response = es.client.prepareSearch(index)
        .setTypes(type)           
        .setQuery(qb)
        .setSize(500)
        .execute()
        .actionGet();
    
    DecimalFormat twoDForm = new DecimalFormat("#.##");
    for (SearchHit hit : response.getHits().getHits()) {
      Map<String,Object> result = hit.getSource();
      Double relevance = Double.valueOf(twoDForm.format(hit.getScore()));
      String shortName = (String) result.get("Dataset-ShortName");
      String longName = (String) result.get("Dataset-LongName");
      @SuppressWarnings("unchecked")
      ArrayList<String> topicList = (ArrayList<String>) result.get("DatasetParameter-Variable");
      String topic = String.join(", ", topicList);
      String content = (String) result.get("Dataset-Description");
      @SuppressWarnings("unchecked")
      ArrayList<String> longdate = (ArrayList<String>) result.get("DatasetCitation-ReleaseDateLong");

      Date date=new Date(Long.valueOf(longdate.get(0)).longValue());
      SimpleDateFormat df2 = new SimpleDateFormat("dd/MM/yy");
      String dateText = df2.format(date);

      SResult re = new SResult(shortName, longName, topic, content, dateText);
      re.setRelevance(relevance);
      re.setDateLong(Long.valueOf(longdate.get(0)).longValue());
      
      /***************************set click count*********************************/
      QueryBuilder qb_clicks = dp.createQueryForClicks(selected_Map, shortName);
      SearchResponse clicks_res = es.client.prepareSearch(index)
          .setTypes(config.get("clickstreamMatrixType"))            
          .setQuery(qb_clicks)
          .setSize(500)
          .execute()
          .actionGet();

      Double click_count = (double) 0;
      for (SearchHit item : clicks_res.getHits().getHits()) {
        Map<String,Object> click = item.getSource();
        Double click_frequency = Double.parseDouble((String) click.get("clicks"));
        String query_str = (String) click.get("query");
        Double query_weight = selected_Map.get(query_str);
        click_count += click_frequency * query_weight;
      }
      re.setClicks(click_count);
      /***************************************************************************/
      resultList.add(re); 
    }
    
    return resultList;
  }
  
  public static void main(String[] args) {
    MudrodEngine mudrod = new MudrodEngine();
    Searcher sr = new Searcher(mudrod.getConfig(), mudrod.getES(), null);
    //dp.createSemQuery("ocean wind", 2);
  }
}
