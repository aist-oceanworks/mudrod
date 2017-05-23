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
import gov.nasa.jpl.mudrod.weblog.structure.RequestUrl;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.StatsAggregationBuilder;
import org.joda.time.DateTime;
import org.joda.time.Seconds;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Supports ability to post-process session, including summarizing statistics
 * and filtering
 */
public class SessionStatistic extends LogAbstract {

  /**
   *
   */
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(SessionStatistic.class);

  public SessionStatistic(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  @Override
  public Object execute() {
    LOG.info("Starting Session Summarization.");
    startTime = System.currentTimeMillis();
    try {
      processSession();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
    endTime = System.currentTimeMillis();
    es.refreshIndex();
    LOG.info("Session Summarization complete. Time elapsed {} seconds.", (endTime - startTime) / 1000);
    return null;
  }

  public void processSession() throws InterruptedException, IOException, ExecutionException {
    String processingType = props.getProperty(MudrodConstants.PROCESS_TYPE);
    if (processingType.equals("sequential")) {
      processSessionInSequential();
    } else if (processingType.equals("parallel")) {
      processSessionInParallel();
    }
  }

  public void processSessionInSequential() throws IOException, InterruptedException, ExecutionException {
    es.createBulkProcessor();
    Terms Sessions = this.getSessionTerms();
    int session_count = 0;
    for (Terms.Bucket entry : Sessions.getBuckets()) {
      if (entry.getDocCount() >= 3 && !entry.getKey().equals("invalid")) {
        String sessionid = entry.getKey().toString();
        int sessionNum = processSession(es, sessionid);
        session_count += sessionNum;
      }
    }
    LOG.info("Final Session count: {}", Integer.toString(session_count));
    es.destroyBulkProcessor();
  }

  /**
   * Extract the dataset ID from a long request
   *
   * @param request raw log request
   * @return dataset ID
   */
  public String findDataset(String request) {
    String pattern1 = "/dataset/";
    String pattern2;
    if (request.contains("?")) {
      pattern2 = "?";
    } else {
      pattern2 = " ";
    }

    Pattern p = Pattern.compile(Pattern.quote(pattern1) + "(.*?)" + Pattern.quote(pattern2));
    Matcher m = p.matcher(request);
    if (m.find()) {
      return m.group(1);
    }
    return null;
  }

  public void processSessionInParallel() throws InterruptedException, IOException {

    List<String> sessions = this.getSessions();
    JavaRDD<String> sessionRDD = spark.sc.parallelize(sessions, partition);

    int sessionCount = 0;
    sessionCount = sessionRDD.mapPartitions(new FlatMapFunction<Iterator<String>, Integer>() {
      @Override
      public Iterator<Integer> call(Iterator<String> arg0) throws Exception {
        ESDriver tmpES = new ESDriver(props);
        tmpES.createBulkProcessor();
        List<Integer> sessionNums = new ArrayList<Integer>();
        sessionNums.add(0);
        while (arg0.hasNext()) {
          String s = arg0.next();
          Integer sessionNum = processSession(tmpES, s);
          sessionNums.add(sessionNum);
        }
        tmpES.destroyBulkProcessor();
        tmpES.close();
        return sessionNums.iterator();
      }
    }).reduce(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer a, Integer b) {
        return a + b;
      }
    });

    LOG.info("Final Session count: {}", Integer.toString(sessionCount));
  }

  public int processSession(ESDriver es, String sessionId) throws IOException, InterruptedException, ExecutionException {

    String inputType = cleanupType;
    String outputType = sessionStats;

    DateTimeFormatter fmt = ISODateTimeFormat.dateTime();
    String min = null;
    String max = null;
    DateTime start = null;
    DateTime end = null;
    int duration = 0;
    float request_rate = 0;

    int session_count = 0;
    Pattern pattern = Pattern.compile("get (.*?) http/*");

    StatsAggregationBuilder statsAgg = AggregationBuilders.stats("Stats").field("Time");

    BoolQueryBuilder filter_search = new BoolQueryBuilder();
    filter_search.must(QueryBuilders.termQuery("SessionID", sessionId));

    SearchResponse sr = es.getClient().prepareSearch(logIndex).setTypes(inputType).setQuery(filter_search).addAggregation(statsAgg).execute().actionGet();

    Stats agg = sr.getAggregations().get("Stats");
    min = agg.getMinAsString();
    max = agg.getMaxAsString();
    start = fmt.parseDateTime(min);
    end = fmt.parseDateTime(max);

    duration = Seconds.secondsBetween(start, end).getSeconds();

    int searchDataListRequest_count = 0;
    int searchDataRequest_count = 0;
    int searchDataListRequest_byKeywords_count = 0;
    int ftpRequest_count = 0;
    int keywords_num = 0;

    String IP = null;
    String keywords = "";
    String views = "";
    String downloads = "";

    SearchResponse scrollResp = es.getClient().prepareSearch(logIndex).setTypes(inputType).setScroll(new TimeValue(60000)).setQuery(filter_search).setSize(100).execute().actionGet();

    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> result = hit.getSource();

        String request = (String) result.get("Request");
        String logType = (String) result.get("LogType");
        IP = (String) result.get("IP");
        Matcher matcher = pattern.matcher(request.trim().toLowerCase());
        while (matcher.find()) {
          request = matcher.group(1);
        }

        String datasetlist = "/datasetlist?";
        String dataset = "/dataset/";
        if (request.contains(datasetlist)) {
          searchDataListRequest_count++;

          RequestUrl requestURL = new RequestUrl();
          String infoStr = requestURL.getSearchInfo(request) + ",";
          String info = es.customAnalyzing(props.getProperty("indexName"), infoStr);

          if (!info.equals(",")) {
            if (keywords.equals("")) {
              keywords = keywords + info;
            } else {
              String[] items = info.split(",");
              String[] keywordList = keywords.split(",");
              for (int m = 0; m < items.length; m++) {
                if (!Arrays.asList(keywordList).contains(items[m])) {
                  keywords = keywords + items[m] + ",";
                }
              }
            }
          }

        }
        if (request.startsWith(dataset)) {
          searchDataRequest_count++;
          if (findDataset(request) != null) {
            String view = findDataset(request);

            if ("".equals(views)) {
              views = view;
            } else {
              if (views.contains(view)) {

              } else {
                views = views + "," + view;
              }
            }
          }
        }
        if ("ftp".equals(logType)) {
          ftpRequest_count++;
          String download = "";
          String requestLowercase = request.toLowerCase();
          if (requestLowercase.endsWith(".jpg") == false && requestLowercase.endsWith(".pdf") == false && requestLowercase.endsWith(".txt") == false && requestLowercase.endsWith(".gif") == false) {
            download = request;
          }

          if ("".equals(downloads)) {
            downloads = download;
          } else {
            if (downloads.contains(download)) {

            } else {
              downloads = downloads + "," + download;
            }
          }
        }

      }

      scrollResp = es.getClient().prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
      // Break condition: No hits are returned
      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }
    }

    if (!keywords.equals("")) {
      keywords_num = keywords.split(",").length;
    }

    if (searchDataListRequest_count != 0 && searchDataListRequest_count <= Integer.parseInt(props.getProperty("searchf")) && searchDataRequest_count != 0 && searchDataRequest_count <= Integer
        .parseInt(props.getProperty("viewf")) && ftpRequest_count <= Integer.parseInt(props.getProperty("downloadf"))) {
      String sessionURL = props.getProperty("SessionPort") + props.getProperty("SessionUrl") + "?sessionid=" + sessionId + "&sessionType=" + outputType + "&requestType=" + inputType;
      session_count = 1;

      IndexRequest ir = new IndexRequest(logIndex, outputType).source(
          jsonBuilder().startObject().field("SessionID", sessionId).field("SessionURL", sessionURL).field("Duration", duration).field("Number of Keywords", keywords_num).field("Time", min)
              .field("End_time", max).field("searchDataListRequest_count", searchDataListRequest_count).field("searchDataListRequest_byKeywords_count", searchDataListRequest_byKeywords_count)
              .field("searchDataRequest_count", searchDataRequest_count).field("keywords", es.customAnalyzing(logIndex, keywords)).field("views", views).field("downloads", downloads)
              .field("request_rate", request_rate).field("Comments", "").field("Validation", 0).field("Produceby", 0).field("Correlation", 0).field("IP", IP).endObject());

      es.getBulkProcessor().add(ir);
    }

    return session_count;
  }

  @Override
  public Object execute(Object o) {
    return null;
  }

}
