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
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.Seconds;
import org.elasticsearch.common.joda.time.format.DateTimeFormatter;
import org.elasticsearch.common.joda.time.format.ISODateTimeFormat;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CrawlerDetection extends DiscoveryStepAbstract {
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(CrawlerDetection.class);
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

  public CrawlerDetection(Properties props, ESDriver es,
      SparkDriver spark) {
    super(props, es, spark);
  }

  @Override
  public Object execute() {
    LOG.info("*****************Crawler detection starts******************");
    startTime = System.currentTimeMillis();
    try {
      checkByRate();
    } catch (InterruptedException | IOException e) {
      LOG.error("Erro checking for crawler detection based upon rate and frequency metric.", e);
    }
    endTime = System.currentTimeMillis();
    es.refreshIndex();
    LOG.info("*****************Crawler detection ends******************Took {}s", (endTime - startTime) / 1000);
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

  public void checkByRate() throws InterruptedException, IOException {
    es.createBulkProcesser();

    int rate = Integer.parseInt(props.getProperty("sendingrate"));
    SearchResponse sr = es.client.prepareSearch(props.getProperty("indexName"))
        .setTypes(httpType).setQuery(QueryBuilders.matchAllQuery()).setSize(0)
        .addAggregation(AggregationBuilders.terms("Users").field("IP").size(0))
        .execute().actionGet();
    Terms users = sr.getAggregations().get("Users");

    int user_count = 0;

    Pattern pattern = Pattern.compile("get (.*?) http/*");
    Matcher matcher;
    for (Terms.Bucket entry : users.getBuckets()) {
      FilterBuilder filterSearch = FilterBuilders.boolFilter()
          .must(FilterBuilders.termFilter("IP", entry.getKey()));
      QueryBuilder querySearch = QueryBuilders
          .filteredQuery(QueryBuilders.matchAllQuery(), filterSearch);

      SearchResponse checkRobot = es.client
          .prepareSearch(props.getProperty("indexName")).setTypes(httpType, ftpType)
          .setQuery(querySearch).setSize(0)
          .addAggregation(AggregationBuilders.dateHistogram("by_minute")
              .field("Time").interval(DateHistogram.Interval.MINUTE)
              .order(DateHistogram.Order.COUNT_DESC))
          .execute().actionGet();

      DateHistogram agg = checkRobot.getAggregations().get("by_minute");

      List<? extends Bucket> botList = agg.getBuckets();
      long maxCount = botList.get(0).getDocCount();
      if (maxCount >= rate) {
      } else {
        user_count++;
        DateTime dt1 = null;
        int toLast = 0;
        SearchResponse scrollResp = es.client
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

            es.bulkProcessor.add(ir);
            dt1 = dt2;
          }

          scrollResp = es.client.prepareSearchScroll(scrollResp.getScrollId())
              .setScroll(new TimeValue(600000)).execute().actionGet();
          if (scrollResp.getHits().getHits().length == 0) {
            break;
          }
        }

      }
    }

    es.destroyBulkProcessor();

    LOG.info("User count: {}", Integer.toString(user_count));
  }

  @Override
  public Object execute(Object o) {
    return null;
  }

}
