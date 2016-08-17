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

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
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
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.sort.SortOrder;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.weblog.structure.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionGenerator extends DiscoveryStepAbstract {

  private static final Logger LOG = LoggerFactory.getLogger(SessionGenerator.class);

  public SessionGenerator(Properties props, ESDriver es,
      SparkDriver spark) {
    super(props, es, spark);
  }

  @Override
  public Object execute() {
    LOG.info("*****************Session generating starts******************");
    startTime = System.currentTimeMillis();
    generateSession();
    endTime = System.currentTimeMillis();
    es.refreshIndex();
    LOG.info("*****************Session generating ends******************Took {}s", (endTime - startTime) / 1000);
    return null;
  }

  public void generateSession() {
    try {
      es.createBulkProcesser();
      genSessionByReferer(Integer.parseInt(props.getProperty("timegap")));
      es.destroyBulkProcessor();

      es.createBulkProcesser();
      combineShortSessions(Integer.parseInt(props.getProperty("timegap")));
      es.destroyBulkProcessor();
    } catch (ElasticsearchException e) {
      LOG.error("Error whilst executing bulk processor.", e);
    } catch (IOException e) {
      LOG.error("Error whilst reading configuration.", e);
    }

  }

  public void genSessionByReferer(int timeThres)
      throws ElasticsearchException, IOException {
    SearchResponse sr = es.client.prepareSearch(props.getProperty("indexName"))
        .setTypes(this.cleanupType).setQuery(QueryBuilders.matchAllQuery())
        .setSize(0)
        .addAggregation(AggregationBuilders.terms("Users").field("IP").size(0))
        .execute().actionGet();
    Terms users = sr.getAggregations().get("Users");

    int sessionCount = 0;
    for (Terms.Bucket entry : users.getBuckets()) {

      String startTime = null;
      int sessionCountIn = 0;

      FilterBuilder filterSearch = FilterBuilders.boolFilter()
          .must(FilterBuilders.termFilter("IP", entry.getKey()));
      QueryBuilder querySearch = QueryBuilders
          .filteredQuery(QueryBuilders.matchAllQuery(), filterSearch);

      SearchResponse scrollResp = es.client
          .prepareSearch(props.getProperty("indexName")).setTypes(this.cleanupType)
          .setScroll(new TimeValue(60000)).setQuery(querySearch)
          .addSort("Time", SortOrder.ASC) // important
          // !!
          .setSize(100).execute().actionGet();

      Map<String, Map<String, DateTime>> sessionReqs = new HashMap<>();
      String request = "";
      String referer = "";
      String logType = "";
      String id = "";
      String ip = entry.getKey();
      String indexUrl = "http://podaac.jpl.nasa.gov/";
      DateTime time = null;
      DateTimeFormatter fmt = ISODateTimeFormat.dateTime();

      while (scrollResp.getHits().getHits().length != 0) {
        for (SearchHit hit : scrollResp.getHits().getHits()) {
          Map<String, Object> result = hit.getSource();
          request = (String) result.get("RequestUrl");
          referer = (String) result.get("Referer");
          logType = (String) result.get("LogType");
          time = fmt.parseDateTime((String) result.get("Time"));
          id = hit.getId();

          if (logType.equals("PO.DAAC")) {
            if (referer.equals("-") || referer.equals(indexUrl)
                || !referer.contains(indexUrl)) {
              sessionCount++;
              sessionCountIn++;
              sessionReqs.put(ip + "@" + sessionCountIn,
                  new HashMap<String, DateTime>());
              sessionReqs.get(ip + "@" + sessionCountIn).put(request, time);

              update(props.getProperty("indexName"), this.cleanupType, id,
                  "SessionID", ip + "@" + sessionCountIn);

            } else {
              int count = sessionCountIn;
              int rollbackNum = 0;
              while (true) {
                Map<String, DateTime> requests = sessionReqs
                    .get(ip + "@" + count);
                if (requests == null) {
                  sessionReqs.put(ip + "@" + count,
                      new HashMap<String, DateTime>());
                  sessionReqs.get(ip + "@" + count).put(request, time);
                  update(props.getProperty("indexName"), this.cleanupType, id,
                      "SessionID", ip + "@" + count);

                  break;
                }
                ArrayList<String> keys = new ArrayList<>(
                    requests.keySet());
                boolean bFindRefer = false;

                for (int i = keys.size() - 1; i >= 0; i--) {
                  rollbackNum++;
                  if (keys.get(i).equalsIgnoreCase(referer)) {
                    bFindRefer = true;
                    // threshold,if time interval > 10*
                    // click num, start a new session
                    if (Math.abs(
                        Seconds.secondsBetween(requests.get(keys.get(i)), time)
                        .getSeconds()) < timeThres * rollbackNum) {
                      sessionReqs.get(ip + "@" + count).put(request, time);
                      update(props.getProperty("indexName"), this.cleanupType, id,
                          "SessionID", ip + "@" + count);
                    } else {
                      sessionCount++;
                      sessionCountIn++;
                      sessionReqs.put(ip + "@" + sessionCountIn,
                          new HashMap<String, DateTime>());
                      sessionReqs.get(ip + "@" + sessionCountIn).put(request,
                          time);
                      update(props.getProperty("indexName"), this.cleanupType, id,
                          "SessionID", ip + "@" + sessionCountIn);
                    }

                    break;
                  }
                }

                if (bFindRefer) {
                  break;
                }

                count--;
                if (count < 0) {

                  sessionCount++;
                  sessionCountIn++;

                  sessionReqs.put(ip + "@" + sessionCountIn,
                      new HashMap<String, DateTime>());
                  sessionReqs.get(ip + "@" + sessionCountIn).put(request,
                      time);
                  update(props.getProperty("indexName"), this.cleanupType, id,
                      "SessionID", ip + "@" + sessionCountIn);

                  break;
                }
              }
            }
          } else if ("ftp".equals(logType)) {

            // may affect computation efficiency
            Map<String, DateTime> requests = sessionReqs
                .get(ip + "@" + sessionCountIn);
            if (requests == null) {
              sessionReqs.put(ip + "@" + sessionCountIn,
                  new HashMap<String, DateTime>());
            } else {
              ArrayList<String> keys = new ArrayList<>(requests.keySet());
              int size = keys.size();
              if (Math.abs(
                  Seconds.secondsBetween(requests.get(keys.get(size - 1)), time)
                  .getSeconds()) > timeThres) {
                sessionCount += 1;
                sessionCountIn += 1;
                sessionReqs.put(ip + "@" + sessionCountIn,
                    new HashMap<String, DateTime>());
              }
            }
            sessionReqs.get(ip + "@" + sessionCountIn).put(request, time);
            update(props.getProperty("indexName"), this.cleanupType, id, "SessionID",
                ip + "@" + sessionCountIn);
          }
        }

        scrollResp = es.client.prepareSearchScroll(scrollResp.getScrollId())
            .setScroll(new TimeValue(600000)).execute().actionGet();
      }
    }
  }

  public void combineShortSessions(int Timethres)
      throws ElasticsearchException, IOException {
    SearchResponse sr = es.client.prepareSearch(props.getProperty("indexName"))
        .setTypes(this.cleanupType).setQuery(QueryBuilders.matchAllQuery())
        .addAggregation(AggregationBuilders.terms("Users").field("IP").size(0))
        .execute().actionGet();
    Terms users = sr.getAggregations().get("Users");

    for (Terms.Bucket entry : users.getBuckets()) {
      FilterBuilder filterAll = FilterBuilders.boolFilter()
          .must(FilterBuilders.termFilter("IP", entry.getKey()));
      QueryBuilder queryAll = QueryBuilders
          .filteredQuery(QueryBuilders.matchAllQuery(), filterAll);
      SearchResponse checkAll = es.client
          .prepareSearch(props.getProperty("indexName")).setTypes(this.cleanupType)
          .setScroll(new TimeValue(60000)).setQuery(queryAll).setSize(0)
          .execute().actionGet();

      long all = checkAll.getHits().getTotalHits();

      FilterBuilder filterCheck = FilterBuilders.boolFilter()
          .must(FilterBuilders.termFilter("IP", entry.getKey()))
          .must(FilterBuilders.termFilter("Referer", "-"));
      QueryBuilder queryCheck = QueryBuilders
          .filteredQuery(QueryBuilders.matchAllQuery(), filterCheck);
      SearchResponse checkReferer = es.client
          .prepareSearch(props.getProperty("indexName")).setTypes(this.cleanupType)
          .setScroll(new TimeValue(60000)).setQuery(queryCheck).setSize(0)
          .execute().actionGet();

      long numInvalid = checkReferer.getHits().getTotalHits();

      double invalidRate = (float) (numInvalid / all);

      if (invalidRate >= 0.8 || all < 3) {
        deleteInvalid(entry.getKey());
        continue;
      }

      FilterBuilder filterSearch = FilterBuilders.boolFilter()
          .must(FilterBuilders.termFilter("IP", entry.getKey()));
      QueryBuilder querySearch = QueryBuilders
          .filteredQuery(QueryBuilders.matchAllQuery(), filterSearch);

      MetricsAggregationBuilder statsAgg = AggregationBuilders.stats("Stats")
          .field("Time");
      SearchResponse sr_session = es.client
          .prepareSearch(props.getProperty("indexName")).setTypes(this.cleanupType)
          .setScroll(new TimeValue(60000)).setQuery(querySearch)
          .addAggregation(AggregationBuilders.terms("Sessions")
              .field("SessionID").size(0).subAggregation(statsAgg))
          .execute().actionGet();

      Terms sessions = sr_session.getAggregations().get("Sessions");

      List<Session> sessionList = new ArrayList<>();
      for (Terms.Bucket session : sessions.getBuckets()) {
        Stats agg = session.getAggregations().get("Stats");
        Session sess = new Session(props, es, agg.getMinAsString(),
            agg.getMaxAsString(), session.getKey());
        sessionList.add(sess);
      }

      Collections.sort(sessionList);

      DateTimeFormatter fmt = ISODateTimeFormat.dateTime();
      String last = null;
      String lastnewID = null;
      String lastoldID = null;
      String current = null;
      for (Session s : sessionList) {
        current = s.getEndTime();
        if (last != null) {
          if (Seconds.secondsBetween(fmt.parseDateTime(last),
              fmt.parseDateTime(current)).getSeconds() < Timethres) {
            if (lastnewID == null) {
              s.setNewID(lastoldID);
            } else {
              s.setNewID(lastnewID);
            }

            FilterBuilder fs = FilterBuilders.boolFilter()
                .must(FilterBuilders.termFilter("SessionID", s.getID()));
            QueryBuilder qs = QueryBuilders
                .filteredQuery(QueryBuilders.matchAllQuery(), fs);
            SearchResponse scrollResp = es.client
                .prepareSearch(props.getProperty("indexName"))
                .setTypes(this.cleanupType).setScroll(new TimeValue(60000))
                .setQuery(qs).setSize(100).execute().actionGet();
            while (true) {
              for (SearchHit hit : scrollResp.getHits().getHits()) {
                if (lastnewID == null) {
                  update(props.getProperty("indexName"), this.cleanupType,
                      hit.getId(), "SessionID", lastoldID);
                } else {
                  update(props.getProperty("indexName"), this.cleanupType,
                      hit.getId(), "SessionID", lastnewID);
                }
              }

              scrollResp = es.client
                  .prepareSearchScroll(scrollResp.getScrollId())
                  .setScroll(new TimeValue(600000)).execute().actionGet();
              // Break condition: No hits are returned
              if (scrollResp.getHits().getHits().length == 0) {
                break;
              }
            }
          }
          ;
        }
        lastoldID = s.getID();
        lastnewID = s.getNewID();
        last = current;
      }
    }
  }

  public void deleteInvalid(String ip) throws IOException {
    FilterBuilder filterAll = FilterBuilders.boolFilter()
        .must(FilterBuilders.termFilter("IP", ip));
    QueryBuilder queryAll = QueryBuilders
        .filteredQuery(QueryBuilders.matchAllQuery(), filterAll);

    SearchResponse scrollResp = es.client.prepareSearch(props.getProperty("indexName"))
        .setTypes(this.cleanupType).setScroll(new TimeValue(60000))
        .setQuery(queryAll).setSize(100).execute().actionGet();
    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        update(props.getProperty("indexName"), cleanupType, hit.getId(), "SessionID",
            "invalid");
      }

      scrollResp = es.client.prepareSearchScroll(scrollResp.getScrollId())
          .setScroll(new TimeValue(600000)).execute().actionGet();
      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }
    }
  }

  private void update(String index, String type, String id, String field1, Object value1) throws IOException {
    UpdateRequest ur = new UpdateRequest(index, type, id)
        .doc(jsonBuilder().startObject().field(field1, value1).endObject());
    es.bulkProcessor.add(ur);
  }

  @Override
  public Object execute(Object o) {
    return null;
  }

}
