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
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.StatsAggregationBuilder;
import org.elasticsearch.search.sort.SortOrder;
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
import esiptestbed.mudrod.weblog.structure.Session;

/**
 * Supports ability to generate user session by time threshold and referrer
 */
public class SessionGenerator extends DiscoveryStepAbstract {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory
      .getLogger(SessionGenerator.class);

  public SessionGenerator(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  @Override
  public Object execute() {
    LOG.info("Starting Session Generation.");
    startTime = System.currentTimeMillis();
    generateSession();
    endTime = System.currentTimeMillis();
    es.refreshIndex();
    LOG.info("Session generating complete. Time elapsed {} seconds.",
        (endTime - startTime) / 1000);
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

  /**
   * Method to generate session by time threshold and referrer
   * 
   * @param timeThres
   *          value of time threshold (s)
   * @throws ElasticsearchException
   *           ElasticsearchException
   * @throws IOException
   *           IOException
   */
  public void genSessionByReferer(int timeThres)
      throws ElasticsearchException, IOException {

    String indexName = props.getProperty(MudrodConstants.ES_INDEX_NAME);
    int docCount = es.getDocCount(indexName, cleanupType);

    SearchRequestBuilder srBuilder = es.getClient().prepareSearch(indexName)
        .setTypes(this.cleanupType).setQuery(QueryBuilders.matchAllQuery())
        .setSize(0).addAggregation(
            AggregationBuilders.terms("Users").field("IP").size(docCount));

    SearchResponse sr = srBuilder.execute().actionGet();
    Terms users = sr.getAggregations().get("Users");

    int sessionCount = 0;
    for (Terms.Bucket entry : users.getBuckets()) {

      String startTime = null;
      int sessionCountIn = 0;

      BoolQueryBuilder filterSearch = new BoolQueryBuilder();
      filterSearch.must(QueryBuilders.termQuery("IP", entry.getKey()));

      SearchResponse scrollResp = es.getClient()
          .prepareSearch(props.getProperty("indexName"))
          .setTypes(this.cleanupType).setScroll(new TimeValue(60000))
          .setQuery(filterSearch).addSort("Time", SortOrder.ASC).setSize(100)
          .execute().actionGet();

      Map<String, Map<String, DateTime>> sessionReqs = new HashMap<>();
      String request = "";
      String referer = "";
      String logType = "";
      String id = "";
      String ip = entry.getKey().toString();
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
                ArrayList<String> keys = new ArrayList<>(requests.keySet());
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
                      update(props.getProperty("indexName"), this.cleanupType,
                          id, "SessionID", ip + "@" + count);
                    } else {
                      sessionCount++;
                      sessionCountIn++;
                      sessionReqs.put(ip + "@" + sessionCountIn,
                          new HashMap<String, DateTime>());
                      sessionReqs.get(ip + "@" + sessionCountIn).put(request,
                          time);
                      update(props.getProperty("indexName"), this.cleanupType,
                          id, "SessionID", ip + "@" + sessionCountIn);
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
                  sessionReqs.get(ip + "@" + sessionCountIn).put(request, time);
                  update(props.getProperty(MudrodConstants.ES_INDEX_NAME),
                      this.cleanupType, id, "SessionID",
                      ip + "@" + sessionCountIn);

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
            update(props.getProperty("indexName"), this.cleanupType, id,
                "SessionID", ip + "@" + sessionCountIn);
          }
        }

        scrollResp = es.getClient()
            .prepareSearchScroll(scrollResp.getScrollId())
            .setScroll(new TimeValue(600000)).execute().actionGet();
      }
    }

    LOG.info("Initial session count: {}", Integer.toString(sessionCount));
  }

  public void combineShortSessions(int Timethres)
      throws ElasticsearchException, IOException {

    String indexName = props.getProperty(MudrodConstants.ES_INDEX_NAME);
    int docCount = es.getDocCount(indexName, cleanupType);

    SearchRequestBuilder srBuilder = es.getClient().prepareSearch(indexName)
        .setTypes(this.cleanupType).setQuery(QueryBuilders.matchAllQuery())
        .setSize(0)
        .addAggregation(AggregationBuilders.terms("Users").field("IP"));

    SearchResponse sr = srBuilder.execute().actionGet();

    Terms users = sr.getAggregations().get("Users");

    for (Terms.Bucket entry : users.getBuckets()) {
      BoolQueryBuilder filterSearch = new BoolQueryBuilder();
      filterSearch.must(QueryBuilders.termQuery("IP", entry.getKey()));

      SearchResponse checkAll = es.getClient()
          .prepareSearch(props.getProperty("indexName"))
          .setTypes(this.cleanupType).setScroll(new TimeValue(60000))
          .setQuery(filterSearch).setSize(0).execute().actionGet();

      long all = checkAll.getHits().getTotalHits();

      BoolQueryBuilder filterCheck = new BoolQueryBuilder();
      filterCheck.must(QueryBuilders.termQuery("IP", entry.getKey()))
          .must(QueryBuilders.termQuery("Referer", "-"));

      SearchResponse checkReferer = es.getClient()
          .prepareSearch(props.getProperty("indexName"))
          .setTypes(this.cleanupType).setScroll(new TimeValue(60000))
          .setQuery(filterCheck).setSize(0).execute().actionGet();

      long numInvalid = checkReferer.getHits().getTotalHits();

      double invalidRate = numInvalid / all;

      if (invalidRate >= 0.8 || all < 3) {
        deleteInvalid(entry.getKey().toString());
        continue;
      }

      StatsAggregationBuilder statsAgg = AggregationBuilders.stats("Stats")
          .field("Time");
      SearchResponse sr_session = es.getClient()
          .prepareSearch(props.getProperty("indexName"))
          .setTypes(this.cleanupType).setScroll(new TimeValue(60000))
          .setQuery(filterSearch)
          .addAggregation(AggregationBuilders.terms("Sessions")
              .field("SessionID")/*.size(0)*/.subAggregation(statsAgg))
          .execute().actionGet();

      Terms sessions = sr_session.getAggregations().get("Sessions");

      List<Session> sessionList = new ArrayList<>();
      for (Terms.Bucket session : sessions.getBuckets()) {
        Stats agg = session.getAggregations().get("Stats");
        Session sess = new Session(props, es, agg.getMinAsString(),
            agg.getMaxAsString(), session.getKey().toString());
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

            QueryBuilder fs = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery("SessionID", s.getID()));

            SearchResponse scrollResp = es.getClient()
                .prepareSearch(props.getProperty("indexName"))
                .setTypes(this.cleanupType).setScroll(new TimeValue(60000))
                .setQuery(fs).setSize(100).execute().actionGet();
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

              scrollResp = es.getClient()
                  .prepareSearchScroll(scrollResp.getScrollId())
                  .setScroll(new TimeValue(600000)).execute().actionGet();
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

  /**
   * Method to remove invalid logs through IP address
   * 
   * @param ip
   *          invalid IP address
   * @throws ElasticsearchException
   *           ElasticsearchException
   * @throws IOException
   *           IOException
   */
  public void deleteInvalid(String ip) throws IOException {
    QueryBuilder filterAll = QueryBuilders.boolQuery()
        .filter(QueryBuilders.termQuery("IP", ip));

    SearchResponse scrollResp = es.getClient()
        .prepareSearch(props.getProperty("indexName"))
        .setTypes(this.cleanupType).setScroll(new TimeValue(60000))
        .setQuery(filterAll).setSize(100).execute().actionGet();
    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        update(props.getProperty("indexName"), cleanupType, hit.getId(),
            "SessionID", "invalid");
      }

      scrollResp = es.getClient().prepareSearchScroll(scrollResp.getScrollId())
          .setScroll(new TimeValue(600000)).execute().actionGet();
      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }
    }
  }

  /**
   * Method to update a Elasticsearch record/document by id, field, and value
   * 
   * @param index
   *          index name is Elasticsearch
   * @param type
   *          type name
   * @param id
   *          ID of the document that needs to be updated
   * @param field1
   *          field of the document that needs to be updated
   * @param value1
   *          value of the document that needs to be changed to
   * @throws ElasticsearchException
   * @throws IOException
   */
  private void update(String index, String type, String id, String field1,
      Object value1) throws IOException {
    UpdateRequest ur = new UpdateRequest(index, type, id)
        .doc(jsonBuilder().startObject().field(field1, value1).endObject());
    es.getBulkProcessor().add(ur);
  }

  @Override
  public Object execute(Object o) {
    return null;
  }

}
