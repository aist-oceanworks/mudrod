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

public class SessionGenerator extends DiscoveryStepAbstract {

  public SessionGenerator(Map<String, String> config, ESDriver es,
      SparkDriver spark) {
    super(config, es, spark);
    // TODO Auto-generated constructor stub
  }

  @Override
  public Object execute() {
    // TODO Auto-generated method stub
    System.out.println(
        "*****************Session generating starts******************");
    startTime = System.currentTimeMillis();
    generateSession();
    endTime = System.currentTimeMillis();
    es.refreshIndex();
    System.out.println(
        "*****************Session generating ends******************Took "
            + (endTime - startTime) / 1000 + "s");
    return null;
  }

  public void generateSession() {
    try {
      es.createBulkProcesser();
      genSessionByReferer(Integer.parseInt(config.get("timegap")));
      es.destroyBulkProcessor();

      es.createBulkProcesser();
      combineShortSessions(Integer.parseInt(config.get("timegap")));
      es.destroyBulkProcessor();
    } catch (ElasticsearchException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }

  public void genSessionByReferer(int Timethres)
      throws ElasticsearchException, IOException {
    SearchResponse sr = es.client.prepareSearch(config.get("indexName"))
        .setTypes(this.Cleanup_type).setQuery(QueryBuilders.matchAllQuery())
        .setSize(0)
        .addAggregation(AggregationBuilders.terms("Users").field("IP").size(0))
        .execute().actionGet();
    Terms Users = sr.getAggregations().get("Users");

    int session_count = 0;
    for (Terms.Bucket entry : Users.getBuckets()) {

      String start_time = null;
      int session_count_in = 0;

      FilterBuilder filter_search = FilterBuilders.boolFilter()
          .must(FilterBuilders.termFilter("IP", entry.getKey()));
      QueryBuilder query_search = QueryBuilders
          .filteredQuery(QueryBuilders.matchAllQuery(), filter_search);

      SearchResponse scrollResp = es.client
          .prepareSearch(config.get("indexName")).setTypes(this.Cleanup_type)
          .setScroll(new TimeValue(60000)).setQuery(query_search)
          .addSort("Time", SortOrder.ASC) // important
          // !!
          .setSize(100).execute().actionGet();

      Map<String, Map<String, DateTime>> sessionReqs = new HashMap<String, Map<String, DateTime>>();
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
              session_count++;
              session_count_in++;
              sessionReqs.put(ip + "@" + session_count_in,
                  new HashMap<String, DateTime>());
              sessionReqs.get(ip + "@" + session_count_in).put(request, time);

              update(config.get("indexName"), this.Cleanup_type, id,
                  "SessionID", ip + "@" + session_count_in);

            } else {
              int count = session_count_in;
              int rollbackNum = 0;
              while (true) {
                Map<String, DateTime> requests = (Map<String, DateTime>) sessionReqs
                    .get(ip + "@" + count);
                if (requests == null) {
                  sessionReqs.put(ip + "@" + count,
                      new HashMap<String, DateTime>());
                  sessionReqs.get(ip + "@" + count).put(request, time);
                  update(config.get("indexName"), this.Cleanup_type, id,
                      "SessionID", ip + "@" + count);

                  break;
                }
                ArrayList<String> keys = new ArrayList<String>(
                    requests.keySet());
                boolean bFindRefer = false;

                for (int i = keys.size() - 1; i >= 0; i--) {
                  rollbackNum++;
                  if (keys.get(i).equals(referer.toLowerCase())) {
                    bFindRefer = true;
                    // threshold,if time interval > 10*
                    // click num, start a new session
                    if (Math.abs(
                        Seconds.secondsBetween(requests.get(keys.get(i)), time)
                            .getSeconds()) < Timethres * rollbackNum) {
                      sessionReqs.get(ip + "@" + count).put(request, time);
                      update(config.get("indexName"), this.Cleanup_type, id,
                          "SessionID", ip + "@" + count);
                    } else {
                      session_count++;
                      session_count_in++;
                      sessionReqs.put(ip + "@" + session_count_in,
                          new HashMap<String, DateTime>());
                      sessionReqs.get(ip + "@" + session_count_in).put(request,
                          time);
                      update(config.get("indexName"), this.Cleanup_type, id,
                          "SessionID", ip + "@" + session_count_in);
                    }

                    break;
                  }
                }

                if (bFindRefer) {
                  break;
                }

                count--;
                if (count < 0) {

                  session_count++;
                  session_count_in++;

                  sessionReqs.put(ip + "@" + session_count_in,
                      new HashMap<String, DateTime>());
                  sessionReqs.get(ip + "@" + session_count_in).put(request,
                      time);
                  update(config.get("indexName"), this.Cleanup_type, id,
                      "SessionID", ip + "@" + session_count_in);

                  break;
                }
              }
            }
          } else if (logType.equals("ftp")) {

            // may affect computation efficiency
            Map<String, DateTime> requests = (Map<String, DateTime>) sessionReqs
                .get(ip + "@" + session_count_in);
            if (requests == null) {
              sessionReqs.put(ip + "@" + session_count_in,
                  new HashMap<String, DateTime>());
            } else {
              ArrayList<String> keys = new ArrayList<String>(requests.keySet());
              int size = keys.size();
              // System.out.println(Math.abs(Seconds.secondsBetween(requests.get(keys.get(size-1)),
              // time).getSeconds()));
              if (Math.abs(
                  Seconds.secondsBetween(requests.get(keys.get(size - 1)), time)
                      .getSeconds()) > Timethres) {
                // System.out.println("new session");
                session_count += 1;
                session_count_in += 1;
                sessionReqs.put(ip + "@" + session_count_in,
                    new HashMap<String, DateTime>());
              }
            }
            sessionReqs.get(ip + "@" + session_count_in).put(request, time);
            update(config.get("indexName"), this.Cleanup_type, id, "SessionID",
                ip + "@" + session_count_in);
          }
        }

        scrollResp = es.client.prepareSearchScroll(scrollResp.getScrollId())
            .setScroll(new TimeValue(600000)).execute().actionGet();
      }
    }

    /*
     * System.out.print("Update is done\n"); System.out.print(
     * "The number of sessions:" + Integer.toString(session_count));
     */
  }

  public void combineShortSessions(int Timethres)
      throws ElasticsearchException, IOException {
    SearchResponse sr = es.client.prepareSearch(config.get("indexName"))
        .setTypes(this.Cleanup_type).setQuery(QueryBuilders.matchAllQuery())
        .addAggregation(AggregationBuilders.terms("Users").field("IP").size(0))
        .execute().actionGet();
    Terms Users = sr.getAggregations().get("Users");

    for (Terms.Bucket entry : Users.getBuckets()) {
      FilterBuilder filter_all = FilterBuilders.boolFilter()
          .must(FilterBuilders.termFilter("IP", entry.getKey()));
      QueryBuilder query_all = QueryBuilders
          .filteredQuery(QueryBuilders.matchAllQuery(), filter_all);
      SearchResponse check_all = es.client
          .prepareSearch(config.get("indexName")).setTypes(this.Cleanup_type)
          .setScroll(new TimeValue(60000)).setQuery(query_all).setSize(0)
          .execute().actionGet();

      long all = check_all.getHits().getTotalHits();

      FilterBuilder filter_check = FilterBuilders.boolFilter()
          .must(FilterBuilders.termFilter("IP", entry.getKey()))
          .must(FilterBuilders.termFilter("Referer", "-"));
      QueryBuilder query_check = QueryBuilders
          .filteredQuery(QueryBuilders.matchAllQuery(), filter_check);
      SearchResponse check_referer = es.client
          .prepareSearch(config.get("indexName")).setTypes(this.Cleanup_type)
          .setScroll(new TimeValue(60000)).setQuery(query_check).setSize(0)
          .execute().actionGet();

      long num_invalid = check_referer.getHits().getTotalHits();

      double invalid_rate = (float) (num_invalid / all);

      if (invalid_rate >= 0.8 || all < 3) {
        deleteInvalid(entry.getKey());
        // System.out.print(entry.getKey() + "\n");
        continue;
      }

      FilterBuilder filter_search = FilterBuilders.boolFilter()
          .must(FilterBuilders.termFilter("IP", entry.getKey()));
      QueryBuilder query_search = QueryBuilders
          .filteredQuery(QueryBuilders.matchAllQuery(), filter_search);

      MetricsAggregationBuilder StatsAgg = AggregationBuilders.stats("Stats")
          .field("Time");
      SearchResponse sr_session = es.client
          .prepareSearch(config.get("indexName")).setTypes(this.Cleanup_type)
          .setScroll(new TimeValue(60000)).setQuery(query_search)
          .addAggregation(AggregationBuilders.terms("Sessions")
              .field("SessionID").size(0).subAggregation(StatsAgg))
          .execute().actionGet();

      Terms Sessions = sr_session.getAggregations().get("Sessions");

      List<Session> sessionList = new ArrayList<Session>();
      for (Terms.Bucket session : Sessions.getBuckets()) {
        Stats agg = session.getAggregations().get("Stats");
        Session sess = new Session(config, es, agg.getMinAsString(),
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
                .prepareSearch(config.get("indexName"))
                .setTypes(this.Cleanup_type).setScroll(new TimeValue(60000))
                .setQuery(qs).setSize(100).execute().actionGet();
            while (true) {
              for (SearchHit hit : scrollResp.getHits().getHits()) {
                if (lastnewID == null) {
                  update(config.get("indexName"), this.Cleanup_type,
                      hit.getId(), "SessionID", lastoldID);
                } else {
                  update(config.get("indexName"), this.Cleanup_type,
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

      // System.out.print(entry.getKey() + "\n");
    }
    // System.out.print("Combining is done.\n");
  }

  public void deleteInvalid(String ip)
      throws ElasticsearchException, IOException {
    FilterBuilder filter_all = FilterBuilders.boolFilter()
        .must(FilterBuilders.termFilter("IP", ip));
    QueryBuilder query_all = QueryBuilders
        .filteredQuery(QueryBuilders.matchAllQuery(), filter_all);

    SearchResponse scrollResp = es.client.prepareSearch(config.get("indexName"))
        .setTypes(this.Cleanup_type).setScroll(new TimeValue(60000))
        .setQuery(query_all).setSize(100).execute().actionGet();
    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        /*
         * DeleteResponse response = Ek_test.client.prepareDelete(this.index,
         * this.cleanup_type, hit.getId()) .setOperationThreaded(false) .get();
         */
        update(config.get("indexName"), Cleanup_type, hit.getId(), "SessionID",
            "invalid");
      }

      scrollResp = es.client.prepareSearchScroll(scrollResp.getScrollId())
          .setScroll(new TimeValue(600000)).execute().actionGet();
      // Break condition: No hits are returned
      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }
    }
  }

  private void update(String index, String type, String id, String field1,
      Object value1) throws ElasticsearchException, IOException {
    UpdateRequest ur = new UpdateRequest(index, type, id)
        .doc(jsonBuilder().startObject().field(field1, value1).endObject());
    es.bulkProcessor.add(ur);
  }

  @Override
  public Object execute(Object o) {
    // TODO Auto-generated method stub
    return null;
  }

}
