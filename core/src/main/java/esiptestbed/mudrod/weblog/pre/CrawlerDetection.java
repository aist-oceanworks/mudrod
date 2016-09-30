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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.joda.time.DateTime;
import org.joda.time.Seconds;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
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

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.main.MudrodConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract} implementation
 * which detects a known list of Web crawlers which may may be present within, and 
 * pollute various logs acting as input to Mudrod.
 */
public class CrawlerDetection extends DiscoveryStepAbstract {
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
   * {@link esiptestbed.mudrod.weblog.pre.CrawlerDetection}
   * @param props populated {@link java.util.Properties} object
   * @param es {@link esiptestbed.mudrod.driver.ESDriver} object to use in crawler detection preprocessing.
   * @param spark {@link esiptestbed.mudrod.driver.SparkDriver} object to use in crawler detection preprocessing.
   */
  public CrawlerDetection(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  @Override
  public Object execute() {
    LOG.info("Starting Crawler detection.");
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
   * @param agent name of a log line
   * @return 1 if the log is initiated by crawler, 0 otherwise
   */
  public boolean checkKnownCrawler(String agent) {
    agent = agent.toLowerCase();
    if (agent.contains(CRAWLER) || agent.contains(GOOGLE_BOT)
        || agent.contains(BING_BOT) || agent.contains(APACHE_HHTP)
        || agent.contains(PERL_BOT) || agent.contains(YAHOO_BOT)
        || agent.contains(YANDEX_BOT) || agent.contains(NO_AGENT_BOT)       
        || agent.contains(PERL_BOT) || agent.contains(APACHE_HHTP)
        || agent.contains(JAVA_CLIENT) || agent.contains(CURL)) {
      return true;
    } else {
      return false;
    }

  }

  /**
   * Check crawler by request sending rate, which is read from configruation file
   * @throws InterruptedException InterruptedException
   * @throws IOException IOException
   */
  public void checkByRate() throws InterruptedException, IOException {
    es.createBulkProcessor();

    int rate = Integer.parseInt(props.getProperty("sendingrate"));
    SearchResponse sr = es.getClient().prepareSearch(props.getProperty(MudrodConstants.ES_INDEX_NAME))
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
      QueryBuilder querySearch = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), filterSearch);

      AggregationBuilder aggregation = AggregationBuilders
          .dateHistogram("by_minute")
          .field("Time")
          .interval(DateHistogramInterval.MINUTE)
          .order(Order.COUNT_DESC);
      SearchResponse checkRobot = es.getClient()
          .prepareSearch(props.getProperty("indexName")).setTypes(httpType, ftpType)
          .setQuery(querySearch).setSize(0)
          .addAggregation(aggregation)
          .execute().actionGet();

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
              result.put("RequestUrl", (String) result.get("Request"));
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

          scrollResp = es.getClient().prepareSearchScroll(scrollResp.getScrollId())
              .setScroll(new TimeValue(600000)).execute().actionGet();
          if (scrollResp.getHits().getHits().length == 0) {
            break;
          }
        }

      }
    }
    es.destroyBulkProcessor();
    LOG.info("User count: {}", Integer.toString(userCount));
  }

  @Override
  public Object execute(Object o) {
    return null;
  }

}
