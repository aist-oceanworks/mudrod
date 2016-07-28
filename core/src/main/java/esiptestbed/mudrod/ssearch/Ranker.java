package esiptestbed.mudrod.ssearch;

import java.util.Map;
import java.util.ArrayList;
import java.util.List;

import esiptestbed.mudrod.discoveryengine.MudrodAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.ssearch.structure.SResult;

public class Ranker extends MudrodAbstract {
  List<SResult> resultList = new ArrayList<SResult>();
  public Ranker(Map<String, String> config, ESDriver es, SparkDriver spark) {
    super(config, es, spark);
    // TODO Auto-generated constructor stub
  }

  public void setResultList(List<SResult> rl)
  {
    resultList = rl;
  }

  double getMean(String attribute)
  {
    Double sum = 0.0;
    for(SResult a : resultList)
      sum += (Double)SResult.get(a, attribute);
    return sum/resultList.size();
  }

  double getVariance(String attribute)
  {
    double mean = getMean(attribute);
    Double temp = 0.0;
    for(SResult a :resultList)
      temp += (mean-(Double)SResult.get(a, attribute))*(mean-(Double)SResult.get(a, attribute));
    return temp/resultList.size();
  }

  double getStdDev(String attribute)
  {
    return Math.sqrt(getVariance(attribute));
  }

  public void generateStats()
  {
    double relevance_mean = getMean("relevance");
    for(int i=0; i< resultList.size(); i++)
    {

    }
  }


  /*  public List<Result> addRecency(String query){
    List<Result> resultList = new ArrayList<Result>();

    String index = config.get("indexName");
    String type = config.get("raw_metadataType");

    //BoolQueryBuilder qb = createSemQuery();
    BoolQueryBuilder qb = null;

    SearchResponse response = es.client.prepareSearch(index)
        .setTypes(type)           
        .setQuery(qb)
        .setSize(500)
        .addSort("DatasetCitation-ReleaseDateLong", SortOrder.DESC)
        .execute()
        .actionGet();

    int i = 1;

    for (SearchHit hit : response.getHits().getHits()) {
      Map<String,Object> result = hit.getSource();
      String shortName = (String) result.get("Dataset-ShortName");

      Result re = new Result(shortName, (float)i, null, null, null, null, "recency");
      resultList.add(re);
      i++;
    }

    return resultList;
  }*/

}
