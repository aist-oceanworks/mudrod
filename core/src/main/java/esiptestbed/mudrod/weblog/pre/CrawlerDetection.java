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
package esiptestbed.mudrod.weblog.pre;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Order;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.joda.time.DateTime;
import org.joda.time.Seconds;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.main.MudrodConstants;

/**
 * An {@link esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract}
 * implementation which detects a known list of Web crawlers which may may be
 * present within, and pollute various logs acting as input to Mudrod.
 */
public class CrawlerDetection extends DiscoveryStepAbstract {
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory
      .getLogger(CrawlerDetection.class);
  public static final String GOOGLE_BOT = "gsa-crawler (Enterprise; T4-JPDGU3TRCQAXZ; earthdata-sa@lists.nasa.gov,srinivasa.s.tummala@nasa.gov)";
  public static final String GOOGLE_BOT_21 = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)";
  public static final String BING_BOT = "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)";
  public static final String YAHOO_BOT = "Mozilla/5.0 (compatible; Yahoo! Slurp; http://help.yahoo.com/help/us/ysearch/slurp)";
  public static final String ROGER_BOT = "rogerbot/1.0 (http://www.moz.com/dp/rogerbot, rogerbot-crawler@moz.com)";
  public static final String YACY_BOT = "yacybot (/global; amd64 Windows Server 2008 R2 6.1; java 1.8.0_31; Europe/de) http://yacy.net/bot.html";
  public static final String YANDEX_BOT = "Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)";
  public static final String GOOGLE_IMAGE = "Googlebot-Image/1.0";
  public static final String RANDOMBOT_1 = "Mozilla/5.0 (iPhone; CPU iPhone OS 6_0 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/6.0 Mobile/10A5376e Safari/8536.25 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)";
  public static final String NO_AGENT_BOT = "-";
  public static final String PERL_BOT = "libwww-perl/";
  public static final String APACHE_HHTP = "Apache-HttpClient/";
  public static final String JAVA_CLIENT = "Java/";
  public static final String CURL = "curl/";

  /**
   * Paramterized constructor to instantiate a configured instance of
   * {@link esiptestbed.mudrod.weblog.pre.CrawlerDetection}
   * 
   * @param props
   *          populated {@link java.util.Properties} object
   * @param es
   *          {@link esiptestbed.mudrod.driver.ESDriver} object to use in
   *          crawler detection preprocessing.
   * @param spark
   *          {@link esiptestbed.mudrod.driver.SparkDriver} object to use in
   *          crawler detection preprocessing.
   */
  public CrawlerDetection(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  public CrawlerDetection() {
    super(null, null, null);
  }

  @Override
  public Object execute() {
    LOG.info("*****************Crawler detection starts******************");
    startTime = System.currentTimeMillis();
    try {
      checkByRate();
    } catch (InterruptedException | IOException e) {
      LOG.error(
          "Erro checking for crawler detection based upon rate and frequency metric.",
          e);
    }
    endTime = System.currentTimeMillis();
    es.refreshIndex();
    LOG.info(
        "*****************Crawler detection ends******************Took {}s",
        (endTime - startTime) / 1000);
    return null;
  }

  public boolean checkKnownCrawler(String agent) {
    if (agent.equals(GOOGLE_BOT) || agent.equals(GOOGLE_BOT_21)
        || agent.equals(BING_BOT) || agent.equals(YAHOO_BOT)
        || agent.equals(ROGER_BOT) || agent.equals(YACY_BOT)
        || agent.equals(YANDEX_BOT) || agent.equals(GOOGLE_IMAGE)
        || agent.equals(RANDOMBOT_1) || agent.equals(NO_AGENT_BOT)
        || agent.contains(PERL_BOT) || agent.contains(APACHE_HHTP)
        || agent.contains(JAVA_CLIENT) || agent.contains(CURL)) {
      return true;
    } else {
      return false;
    }
  }

  private List<String> getAllUsers() {

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

  public void checkByRate() throws InterruptedException, IOException {
    es.createBulkProcesser();

    int userCount = 0;
    List<String> users = this.getAllUsers();
    JavaRDD<String> userRDD = spark.sc.parallelize(users);

    Broadcast<ESDriver> broadcastVar = spark.sc.broadcast(es);
    userRDD.foreach(new VoidFunction<String>() {
      @Override
      public void call(String arg0) throws Exception {
        // TODO Auto-generated method stub
        checkByRate(broadcastVar.value(), arg0);
      }
    });

    LOG.info("User count: {}", Integer.toString(userCount));
  }

  private boolean checkByRate(ESDriver es, String user) {

    // System.out.println(es);
    int rate = Integer.parseInt(props.getProperty("sendingrate"));
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

    return true;
  }

  /* 
  public void checkByRate() throws InterruptedException, IOException {
    es.createBulkProcesser();
  
    int rate = Integer.parseInt(props.getProperty("sendingrate"));
    SearchResponse sr = es.getClient()
        .prepareSearch(props.getProperty(MudrodConstants.ES_INDEX_NAME))
        .setTypes(httpType).setQuery(QueryBuilders.matchAllQuery()).setSize(0)
        .addAggregation(AggregationBuilders.terms("Users").field("IP").size(0))
        .execute().actionGet();
    Terms users = sr.getAggregations().get("Users");
  
    int userCount = 0;
  
    Pattern pattern = Pattern.compile("get (.*?) http/*");
    Matcher matcher;
    for (Terms.Bucket entry : users.getBuckets()) {
      QueryBuilder filterSearch = QueryBuilders.boolQuery()
          .must(QueryBuilders.termQuery("IP", entry.getKey()));
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
      } else {
        userCount++;
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
                result.put("RequestUrl",
                    "http://podaac.jpl.nasa.gov" + request);
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
    }
    es.destroyBulkProcessor();
    LOG.info("User count: {}", Integer.toString(userCount));
  }*/

  @Override
  public Object execute(Object o) {
    return null;
  }

}
