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

import gov.nasa.jpl.mudrod.driver.ESDriver;
import gov.nasa.jpl.mudrod.driver.SparkDriver;
import gov.nasa.jpl.mudrod.main.MudrodConstants;
import gov.nasa.jpl.mudrod.weblog.structure.Session;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.elasticsearch.ElasticsearchException;
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

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Supports ability to generate user session by time threshold and referrer
 */
public class SessionGenerator extends LogAbstract {

  /**
   *
   */
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(SessionGenerator.class);

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
    LOG.info("Session generating complete. Time elapsed {} seconds.", (endTime - startTime) / 1000);
    return null;
  }

  public void generateSession() {
    try {
      es.createBulkProcessor();
      genSessionByReferer(Integer.parseInt(props.getProperty("timegap")));
      es.destroyBulkProcessor();

      es.createBulkProcessor();
      combineShortSessions(Integer.parseInt(props.getProperty("timegap")));
      es.destroyBulkProcessor();
    } catch (ElasticsearchException e) {
      LOG.error("Error whilst executing bulk processor.", e);
    } catch (IOException e) {
      LOG.error("Error whilst reading configuration.", e);
    } catch (NumberFormatException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void genSessionByReferer(int timeThres) throws InterruptedException, IOException {
    String processingType = props.getProperty(MudrodConstants.PROCESS_TYPE);
    if (processingType.equals("sequential")) {
      genSessionByRefererInSequential(timeThres);
    } else if (processingType.equals("parallel")) {
      genSessionByRefererInParallel(timeThres);
    }
  }

  public void combineShortSessions(int timeThres) throws InterruptedException, IOException {
    String processingType = props.getProperty(MudrodConstants.PROCESS_TYPE);
    if (processingType.equals("sequential")) {
      combineShortSessionsInSequential(timeThres);
    } else if (processingType.equals("parallel")) {
      combineShortSessionsInParallel(timeThres);
    }
  }

  /**
   * Method to generate session by time threshold and referrer
   *
   * @param timeThres value of time threshold (s)
   * @throws ElasticsearchException ElasticsearchException
   * @throws IOException            IOException
   */
  public void genSessionByRefererInSequential(int timeThres) throws ElasticsearchException, IOException {

    Terms users = this.getUserTerms(this.cleanupType);

    int sessionCount = 0;
    for (Terms.Bucket entry : users.getBuckets()) {

      String user = (String) entry.getKey();
      Integer sessionNum = genSessionByReferer(es, user, timeThres);
      sessionCount += sessionNum;
    }

    LOG.info("Initial session count: {}", Integer.toString(sessionCount));
  }

  public void combineShortSessionsInSequential(int timeThres) throws ElasticsearchException, IOException {

    Terms users = this.getUserTerms(this.cleanupType);
    for (Terms.Bucket entry : users.getBuckets()) {
      String user = entry.getKey().toString();
      combineShortSessions(es, user, timeThres);
    }
  }

  /**
   * Method to remove invalid logs through IP address
   *
   * @param es an instantiated es driver
   * @param ip invalid IP address
   * @throws ElasticsearchException ElasticsearchException
   * @throws IOException            IOException
   */
  public void deleteInvalid(ESDriver es, String ip) throws IOException {

    BoolQueryBuilder filterAll = new BoolQueryBuilder();
    filterAll.must(QueryBuilders.termQuery("IP", ip));

    SearchResponse scrollResp = es.getClient().prepareSearch(logIndex).setTypes(this.cleanupType).setScroll(new TimeValue(60000)).setQuery(filterAll).setSize(100).execute().actionGet();
    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        update(es, logIndex, cleanupType, hit.getId(), "SessionID", "invalid");
      }

      scrollResp = es.getClient().prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }
    }
  }

  /**
   * Method to update a Elasticsearch record/document by id, field, and value
   *
   * @param es
   * @param index  index name is Elasticsearch
   * @param type   type name
   * @param id     ID of the document that needs to be updated
   * @param field1 field of the document that needs to be updated
   * @param value1 value of the document that needs to be changed to
   * @throws ElasticsearchException
   * @throws IOException
   */
  private void update(ESDriver es, String index, String type, String id, String field1, Object value1) throws IOException {
    UpdateRequest ur = new UpdateRequest(index, type, id).doc(jsonBuilder().startObject().field(field1, value1).endObject());
    es.getBulkProcessor().add(ur);
  }

  public void genSessionByRefererInParallel(int timeThres) throws InterruptedException, IOException {

    JavaRDD<String> userRDD = getUserRDD(this.cleanupType);

    int sessionCount = 0;
    sessionCount = userRDD.mapPartitions(new FlatMapFunction<Iterator<String>, Integer>() {
      /**
       *
       */
      private static final long serialVersionUID = 1L;

      @Override
      public Iterator<Integer> call(Iterator<String> arg0) throws Exception {
        ESDriver tmpES = new ESDriver(props);
        tmpES.createBulkProcessor();
        List<Integer> sessionNums = new ArrayList<>();
        while (arg0.hasNext()) {
          String s = arg0.next();
          Integer sessionNum = genSessionByReferer(tmpES, s, timeThres);
          sessionNums.add(sessionNum);
        }
        tmpES.destroyBulkProcessor();
        tmpES.close();
        return sessionNums.iterator();
      }
    }).reduce(new Function2<Integer, Integer, Integer>() {
      /**
       *
       */
      private static final long serialVersionUID = 1L;

      @Override
      public Integer call(Integer a, Integer b) {
        return a + b;
      }
    });

    LOG.info("Initial Session count: {}", Integer.toString(sessionCount));
  }

  public int genSessionByReferer(ESDriver es, String user, int timeThres) throws ElasticsearchException, IOException {

    String startTime = null;
    int sessionCountIn = 0;

    BoolQueryBuilder filterSearch = new BoolQueryBuilder();
    filterSearch.must(QueryBuilders.termQuery("IP", user));

    SearchResponse scrollResp = es.getClient().prepareSearch(logIndex).setTypes(this.cleanupType).setScroll(new TimeValue(60000)).setQuery(filterSearch).addSort("Time", SortOrder.ASC).setSize(100)
        .execute().actionGet();

    Map<String, Map<String, DateTime>> sessionReqs = new HashMap<>();
    String request = "";
    String referer = "";
    String logType = "";
    String id = "";
    String ip = user;
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

        if ("PO.DAAC".equals(logType)) {
          if ("-".equals(referer) || referer.equals(indexUrl) || !referer.contains(indexUrl)) {
            sessionCountIn++;
            sessionReqs.put(ip + "@" + sessionCountIn, new HashMap<String, DateTime>());
            sessionReqs.get(ip + "@" + sessionCountIn).put(request, time);

            update(es, logIndex, this.cleanupType, id, "SessionID", ip + "@" + sessionCountIn);

          } else {
            int count = sessionCountIn;
            int rollbackNum = 0;
            while (true) {
              Map<String, DateTime> requests = sessionReqs.get(ip + "@" + count);
              if (requests == null) {
                sessionReqs.put(ip + "@" + count, new HashMap<String, DateTime>());
                sessionReqs.get(ip + "@" + count).put(request, time);
                update(es, logIndex, this.cleanupType, id, "SessionID", ip + "@" + count);

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
                  if (Math.abs(Seconds.secondsBetween(requests.get(keys.get(i)), time).getSeconds()) < timeThres * rollbackNum) {
                    sessionReqs.get(ip + "@" + count).put(request, time);
                    update(es, logIndex, this.cleanupType, id, "SessionID", ip + "@" + count);
                  } else {
                    sessionCountIn++;
                    sessionReqs.put(ip + "@" + sessionCountIn, new HashMap<String, DateTime>());
                    sessionReqs.get(ip + "@" + sessionCountIn).put(request, time);
                    update(es, logIndex, this.cleanupType, id, "SessionID", ip + "@" + sessionCountIn);
                  }

                  break;
                }
              }

              if (bFindRefer) {
                break;
              }

              count--;
              if (count < 0) {
                sessionCountIn++;

                sessionReqs.put(ip + "@" + sessionCountIn, new HashMap<String, DateTime>());
                sessionReqs.get(ip + "@" + sessionCountIn).put(request, time);
                update(es, props.getProperty(MudrodConstants.ES_INDEX_NAME), this.cleanupType, id, "SessionID", ip + "@" + sessionCountIn);

                break;
              }
            }
          }
        } else if ("ftp".equals(logType)) {

          // may affect computation efficiency
          Map<String, DateTime> requests = sessionReqs.get(ip + "@" + sessionCountIn);
          if (requests == null) {
            sessionReqs.put(ip + "@" + sessionCountIn, new HashMap<String, DateTime>());
          } else {
            ArrayList<String> keys = new ArrayList<>(requests.keySet());
            int size = keys.size();
            if (Math.abs(Seconds.secondsBetween(requests.get(keys.get(size - 1)), time).getSeconds()) > timeThres) {
              sessionCountIn += 1;
              sessionReqs.put(ip + "@" + sessionCountIn, new HashMap<String, DateTime>());
            }
          }
          sessionReqs.get(ip + "@" + sessionCountIn).put(request, time);
          update(es, logIndex, this.cleanupType, id, "SessionID", ip + "@" + sessionCountIn);
        }
      }

      scrollResp = es.getClient().prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
    }

    return sessionCountIn;
  }

  public void combineShortSessionsInParallel(int timeThres) throws InterruptedException, IOException {

    JavaRDD<String> userRDD = getUserRDD(this.cleanupType);

    userRDD.foreachPartition(new VoidFunction<Iterator<String>>() {
      /**
       *
       */
      private static final long serialVersionUID = 1L;

      @Override
      public void call(Iterator<String> arg0) throws Exception {
        ESDriver tmpES = new ESDriver(props);
        tmpES.createBulkProcessor();
        while (arg0.hasNext()) {
          String s = arg0.next();
          combineShortSessions(tmpES, s, timeThres);
        }
        tmpES.destroyBulkProcessor();
        tmpES.close();
      }
    });
  }

  public void combineShortSessions(ESDriver es, String user, int timeThres) throws ElasticsearchException, IOException {

    BoolQueryBuilder filterSearch = new BoolQueryBuilder();
    filterSearch.must(QueryBuilders.termQuery("IP", user));

    String[] indexArr = new String[] { logIndex };
    String[] typeArr = new String[] { cleanupType };
    int docCount = es.getDocCount(indexArr, typeArr, filterSearch);

    if (docCount < 3) {
      deleteInvalid(es, user);
      return;
    }

    BoolQueryBuilder filterCheck = new BoolQueryBuilder();
    filterCheck.must(QueryBuilders.termQuery("IP", user)).must(QueryBuilders.termQuery("Referer", "-"));
    SearchResponse checkReferer = es.getClient().prepareSearch(logIndex).setTypes(this.cleanupType).setScroll(new TimeValue(60000)).setQuery(filterCheck).setSize(0).execute().actionGet();

    long numInvalid = checkReferer.getHits().getTotalHits();
    double invalidRate = numInvalid / docCount;

    if (invalidRate >= 0.8) {
      deleteInvalid(es, user);
      return;
    }

    StatsAggregationBuilder statsAgg = AggregationBuilders.stats("Stats").field("Time");
    SearchResponse srSession = es.getClient().prepareSearch(logIndex).setTypes(this.cleanupType).setScroll(new TimeValue(60000)).setQuery(filterSearch)
        .addAggregation(AggregationBuilders.terms("Sessions").field("SessionID").size(docCount).subAggregation(statsAgg)).execute().actionGet();

    Terms sessions = srSession.getAggregations().get("Sessions");

    List<Session> sessionList = new ArrayList<>();
    for (Terms.Bucket session : sessions.getBuckets()) {
      Stats agg = session.getAggregations().get("Stats");
      Session sess = new Session(props, es, agg.getMinAsString(), agg.getMaxAsString(), session.getKey().toString());
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
        if (Seconds.secondsBetween(fmt.parseDateTime(last), fmt.parseDateTime(current)).getSeconds() < timeThres) {
          if (lastnewID == null) {
            s.setNewID(lastoldID);
          } else {
            s.setNewID(lastnewID);
          }

          QueryBuilder fs = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("SessionID", s.getID()));

          SearchResponse scrollResp = es.getClient().prepareSearch(logIndex).setTypes(this.cleanupType).setScroll(new TimeValue(60000)).setQuery(fs).setSize(100).execute().actionGet();
          while (true) {
            for (SearchHit hit : scrollResp.getHits().getHits()) {
              if (lastnewID == null) {
                update(es, logIndex, this.cleanupType, hit.getId(), "SessionID", lastoldID);
              } else {
                update(es, logIndex, this.cleanupType, hit.getId(), "SessionID", lastnewID);
              }
            }

            scrollResp = es.getClient().prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
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

  @Override
  public Object execute(Object o) {
    return null;
  }

}
