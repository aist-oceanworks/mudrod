package esiptestbed.mudrod.weblog.structure;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.main.MudrodConstants;

public class test implements Serializable {

  public void checkByRate(ESDriver es, SparkDriver spark, Properties props,
      String httpType) throws InterruptedException, IOException {
    es.createBulkProcessor();

    List<String> users = this.getAllUsers(es, props, httpType);
    JavaRDD<String> userRDD = spark.sc.parallelize(users);

    int userCount = 0;

    userRDD.foreach(new VoidFunction<String>() {
      @Override
      public void call(String arg0) throws Exception {
        // TODO Auto-generated method stub
        // checkByRate(arg0);
        System.out.println(arg0);
      }
    });

    es.destroyBulkProcessor();
    // LOG.info("User count: {}", Integer.toString(userCount));
  }

  private List<String> getAllUsers(ESDriver es, Properties props,
      String httpType) {

    SearchResponse sr = es.getClient()
        .prepareSearch(props.getProperty(MudrodConstants.ES_INDEX_NAME))
        .setTypes(httpType).setQuery(QueryBuilders.matchAllQuery()).setSize(0)
        .addAggregation(AggregationBuilders.terms("Users").field("IP").size(0))
        .execute().actionGet();
    Terms users = sr.getAggregations().get("Users");
    List<String> userList = new ArrayList<String>();
    for (Terms.Bucket entry : users.getBuckets()) {
      String ip = (String) entry.getKey();
      userList.add(ip);
    }

    return userList;
  }

  public boolean checkByRate(ESDriver es, SparkDriver spark, Properties props,
      String httpType, String ftpType, String cleanupType, String user) {

    System.out.println(es.getClient());

    /* int rate = Integer.parseInt(props.getProperty("sendingrate"));
    Pattern pattern = Pattern.compile("get (.*?) http/*");
    Matcher matcher;
    
    QueryBuilder filterSearch = QueryBuilders.boolQuery()
        .must(QueryBuilders.termQuery("IP", user));
    QueryBuilder querySearch = QueryBuilders
        .filteredQuery(QueryBuilders.matchAllQuery(), filterSearch);
    
    AggregationBuilder aggregation = AggregationBuilders
        .dateHistogram("by_minute").field("Time")
        .interval(DateHistogramInterval.MINUTE).order(Order.COUNT_DESC);
    
    
    SearchResponse checkRobot = es.getClient()
        .prepareSearch(props.getProperty("indexName"))
        .setTypes(httpType, ftpType).setQuery(querySearch).setSize(0)
        .addAggregation(aggregation).execute().actionGet();
    
    Histogram agg = checkRobot.getAggregations().get("by_minute");
    
    List<? extends Histogram.Bucket> botList = agg.getBuckets();
    long maxCount = botList.get(0).getDocCount();
    if (maxCount >= rate) {
      return false;
    } else {
      DateTime dt1 = null;
      int toLast = 0;
      SearchResponse scrollResp = es.getClient()
          .prepareSearch(props.getProperty("indexName"))
          .setTypes(httpType, ftpType).setScroll(new TimeValue(60000))
          .setQuery(querySearch).setSize(100).execute().actionGet();
      while (true) {
        for (SearchHit hit : scrollResp.getHits().getHits()) {
          Map<String, Object> result = hit.getSource();
          String logtype = (String) result.get("LogType");
          if (logtype.equals("PO.DAAC")) {
            String request = (String) result.get("Request");
            matcher = pattern.matcher(request.trim().toLowerCase());
            boolean find = false;
            while (matcher.find()) {
              request = matcher.group(1);
              result.put("RequestUrl", "http://podaac.jpl.nasa.gov" + request);
              find = true;
            }
            if (!find) {
              result.put("RequestUrl", request);
            }
          } else {
            result.put("RequestUrl", result.get("Request"));
          }
    
          DateTimeFormatter fmt = ISODateTimeFormat.dateTime();
          DateTime dt2 = fmt.parseDateTime((String) result.get("Time"));
    
          if (dt1 == null) {
            toLast = 0;
          } else {
            toLast = Math.abs(Seconds.secondsBetween(dt1, dt2).getSeconds());
          }
          result.put("ToLast", toLast);
          IndexRequest ir = new IndexRequest(props.getProperty("indexName"),
              cleanupType).source(result);
    
          es.getBulkProcessor().add(ir);
          dt1 = dt2;
        }
    
        scrollResp = es.getClient()
            .prepareSearchScroll(scrollResp.getScrollId())
            .setScroll(new TimeValue(600000)).execute().actionGet();
        if (scrollResp.getHits().getHits().length == 0) {
          break;
        }
      }
    
    }
    */
    return true;
  }

}
