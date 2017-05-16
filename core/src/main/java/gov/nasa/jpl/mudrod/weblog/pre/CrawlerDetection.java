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
package gov.nasa.jpl.mudrod.weblog.pre;

import gov.nasa.jpl.mudrod.discoveryengine.DiscoveryStepAbstract;
import gov.nasa.jpl.mudrod.driver.ESDriver;
import gov.nasa.jpl.mudrod.driver.SparkDriver;
import gov.nasa.jpl.mudrod.main.MudrodConstants;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
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

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An {@link DiscoveryStepAbstract}
 * implementation which detects a known list of Web crawlers which may may be
 * present within, and pollute various logs acting as input to Mudrod.
 */
public class CrawlerDetection extends LogAbstract {
  /**
   *
   */
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(CrawlerDetection.class);

  public static final String CRAWLER = "crawler";
  public static final String GOOGLE_BOT = "googlebot";
  public static final String BING_BOT = "bingbot";
  public static final String YAHOO_BOT = "slurp";
  public static final String YACY_BOT = "yacybot";
  public static final String ROGER_BOT = "rogerbot";
  public static final String YANDEX_BOT = "yandexbot";

  public static final String NO_AGENT_BOT = "-";
  public static final String PERL_BOT = "libwww-perl/";
  public static final String APACHE_HHTP = "apache-httpclient/";
  public static final String JAVA_CLIENT = "java/";
  public static final String CURL = "curl/";

  /**
   * Paramterized constructor to instantiate a configured instance of
   * {@link CrawlerDetection}
   *
   * @param props populated {@link java.util.Properties} object
   * @param es    {@link ESDriver} object to use in
   *              crawler detection preprocessing.
   * @param spark {@link SparkDriver} object to use in
   *              crawler detection preprocessing.
   */
  public CrawlerDetection(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  public CrawlerDetection() {
    super(null, null, null);
  }

  @Override
  public Object execute() {
    LOG.info("Starting Crawler detection {}.", httpType);
    startTime = System.currentTimeMillis();
    try {
      checkByRate();
    } catch (InterruptedException | IOException e) {
      LOG.error("Encountered an error whilst detecting Web crawlers.", e);
    }
    endTime = System.currentTimeMillis();
    es.refreshIndex();
    LOG.info("Crawler detection complete. Time elapsed {} seconds", (endTime - startTime) / 1000);
    return null;
  }

  /**
   * Check known crawler through crawler agent name list
   *
   * @param agent name of a log line
   * @return 1 if the log is initiated by crawler, 0 otherwise
   */
  public boolean checkKnownCrawler(String agent) {
    agent = agent.toLowerCase();
    if (agent.contains(CRAWLER) || agent.contains(GOOGLE_BOT) || agent.contains(BING_BOT) || agent.contains(APACHE_HHTP) || agent.contains(PERL_BOT) || agent.contains(YAHOO_BOT) || agent
        .contains(YANDEX_BOT) || agent.contains(NO_AGENT_BOT) || agent.contains(PERL_BOT) || agent.contains(APACHE_HHTP) || agent.contains(JAVA_CLIENT) || agent.contains(CURL)) {
      return true;
    } else {
      return false;
    }
  }

  public void checkByRate() throws InterruptedException, IOException {
    String processingType = props.getProperty(MudrodConstants.PROCESS_TYPE);
    if (processingType.equals("sequential")) {
      checkByRateInSequential();
    } else if (processingType.equals("parallel")) {
      checkByRateInParallel();
    }
  }

  /**
   * Check crawler by request sending rate, which is read from configruation
   * file
   *
   * @throws InterruptedException InterruptedException
   * @throws IOException          IOException
   */
  public void checkByRateInSequential() throws InterruptedException, IOException {
    es.createBulkProcessor();

    int rate = Integer.parseInt(props.getProperty("sendingrate"));

    Terms users = this.getUserTerms(this.httpType);
    LOG.info("Original User count: {}", Integer.toString(users.getBuckets().size()));

    int userCount = 0;
    for (Terms.Bucket entry : users.getBuckets()) {
      String user = entry.getKey().toString();
      int count = checkByRate(es, user);
      userCount += count;
    }
    es.destroyBulkProcessor();
    LOG.info("User count: {}", Integer.toString(userCount));
  }

  void checkByRateInParallel() throws InterruptedException, IOException {

    JavaRDD<String> userRDD = getUserRDD(this.httpType);
    LOG.info("Original User count: {}", userRDD.count());

    int userCount = 0;
    userCount = userRDD.mapPartitions((FlatMapFunction<Iterator<String>, Integer>) iterator -> {
      ESDriver tmpES = new ESDriver(props);
      tmpES.createBulkProcessor();
      List<Integer> realUserNums = new ArrayList<>();
      while (iterator.hasNext()) {
        String s = iterator.next();
        Integer realUser = checkByRate(tmpES, s);
        realUserNums.add(realUser);
      }
      tmpES.destroyBulkProcessor();
      tmpES.close();
      return realUserNums.iterator();
    }).reduce((Function2<Integer, Integer, Integer>) (a, b) -> a + b);

    LOG.info("User count: {}", Integer.toString(userCount));
  }

  private int checkByRate(ESDriver es, String user) {

    int rate = Integer.parseInt(props.getProperty("sendingrate"));
    Pattern pattern = Pattern.compile("get (.*?) http/*");
    Matcher matcher;

    BoolQueryBuilder filterSearch = new BoolQueryBuilder();
    filterSearch.must(QueryBuilders.termQuery("IP", user));

    AggregationBuilder aggregation = AggregationBuilders.dateHistogram("by_minute").field("Time").dateHistogramInterval(DateHistogramInterval.MINUTE).order(Order.COUNT_DESC);
    SearchResponse checkRobot = es.getClient().prepareSearch(logIndex).setTypes(httpType, ftpType).setQuery(filterSearch).setSize(0).addAggregation(aggregation).execute().actionGet();

    Histogram agg = checkRobot.getAggregations().get("by_minute");

    List<? extends Histogram.Bucket> botList = agg.getBuckets();
    long maxCount = botList.get(0).getDocCount();
    if (maxCount >= rate) {
      return 0;
    } else {
      DateTime dt1 = null;
      int toLast = 0;
      SearchResponse scrollResp = es.getClient().prepareSearch(logIndex).setTypes(httpType, ftpType).setScroll(new TimeValue(60000)).setQuery(filterSearch).setSize(100).execute().actionGet();
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
          IndexRequest ir = new IndexRequest(logIndex, cleanupType).source(result);

          es.getBulkProcessor().add(ir);
          dt1 = dt2;
        }

        scrollResp = es.getClient().prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
        if (scrollResp.getHits().getHits().length == 0) {
          break;
        }
      }

    }

    return 1;
  }

  @Override
  public Object execute(Object o) {
    return null;
  }

}
