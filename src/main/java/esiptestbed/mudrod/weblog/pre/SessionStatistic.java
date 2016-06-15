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
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse.AnalyzeToken;
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
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.discoveryengine.MudrodAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.weblog.structure.Coordinates;
import esiptestbed.mudrod.weblog.structure.GeoIp;
import esiptestbed.mudrod.weblog.structure.RequestUrl;

public class SessionStatistic extends DiscoveryStepAbstract {
  public SessionStatistic(Map<String, String> config, ESDriver es,
      SparkDriver spark) {
    super(config, es, spark);
    // TODO Auto-generated constructor stub
  }

  @Override
  public Object execute() {
    // TODO Auto-generated method stub
    System.out.println(
        "*****************Session summarizing starts******************");
    startTime = System.currentTimeMillis();
    try {
      processSession();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (ExecutionException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    endTime = System.currentTimeMillis();
    es.refreshIndex();
    System.out.println(
        "*****************Session summarizing ends******************Took "
            + (endTime - startTime) / 1000 + "s");
    return null;
  }

  public void processSession()
      throws IOException, InterruptedException, ExecutionException {
    es.createBulkProcesser();
    String inputType = this.Cleanup_type;
    String outputType = this.SessionStats;

    MetricsAggregationBuilder StatsAgg = AggregationBuilders.stats("Stats")
        .field("Time");
    SearchResponse sr = es.client.prepareSearch(config.get("indexName"))
        .setTypes(inputType).setQuery(QueryBuilders.matchAllQuery())
        .addAggregation(AggregationBuilders.terms("Sessions").field("SessionID")
            .size(0).subAggregation(StatsAgg))
        .execute().actionGet();

    Terms Sessions = sr.getAggregations().get("Sessions");
    DateTimeFormatter fmt = ISODateTimeFormat.dateTime();
    String min = null;
    String max = null;
    DateTime start = null;
    DateTime end = null;
    int duration = 0;
    float request_rate = 0;

    int session_count = 0;
    Pattern pattern = Pattern.compile("get (.*?) http/*");
    for (Terms.Bucket entry : Sessions.getBuckets()) {
      if (entry.getDocCount() >= 3 && !entry.getKey().equals("invalid")) {

        Stats agg = entry.getAggregations().get("Stats");
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
        FilterBuilder filter_search = FilterBuilders.boolFilter()
            .must(FilterBuilders.termFilter("SessionID", entry.getKey()));
        QueryBuilder query_search = QueryBuilders
            .filteredQuery(QueryBuilders.matchAllQuery(), filter_search);

        SearchResponse scrollResp = es.client
            .prepareSearch(config.get("indexName")).setTypes(inputType)
            .setScroll(new TimeValue(60000)).setQuery(query_search).setSize(100)
            .execute().actionGet();

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

              RequestUrl requestURL = new RequestUrl(this.config, this.es,
                  null);
              String info = requestURL.GetSearchInfo(request) + ",";

              if (!info.equals(",")) {
                if (keywords.equals("")) {
                  keywords = keywords + info;
                } else {
                  String[] items = info.split(",");
                  String[] keyword_list = keywords.split(",");
                  for (int m = 0; m < items.length; m++) {
                    if (!Arrays.asList(keyword_list).contains(items[m])) {
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

                if (views.equals("")) {
                  views = view;
                } else {
                  if (views.contains(view)) {

                  } else {
                    views = views + "," + view;
                  }
                }
              }
            }
            if (logType.equals("ftp")) {
              ftpRequest_count++;
              String download = "";
              String request_lowercase = request.toLowerCase();
              if (request_lowercase.endsWith(".jpg") == false
                  && request_lowercase.endsWith(".pdf") == false
                  && request_lowercase.endsWith(".txt") == false
                  && request_lowercase.endsWith(".gif") == false) {
                download = request;
              }

              if (downloads.equals("")) {
                downloads = download;
              } else {
                if (downloads.contains(download)) {

                } else {
                  downloads = downloads + "," + download;
                }
              }
            }

          }

          scrollResp = es.client.prepareSearchScroll(scrollResp.getScrollId())
              .setScroll(new TimeValue(600000)).execute().actionGet();
          // Break condition: No hits are returned
          if (scrollResp.getHits().getHits().length == 0) {
            break;
          }
        }

        if (!keywords.equals("")) {
          keywords_num = keywords.split(",").length;
        }

        // if (searchDataListRequest_count != 0 && searchDataRequest_count != 0
        // && ftpRequest_count != 0 && keywords_num<50) {
        if (searchDataListRequest_count != 0
            && searchDataListRequest_count <= Integer
                .parseInt(config.get("searchf"))
            && searchDataRequest_count != 0
            && searchDataRequest_count <= Integer.parseInt(config.get("viewf"))
            && ftpRequest_count <= Integer.parseInt(config.get("downloadf"))) {
          // GeoIp converter = new GeoIp();
          // Coordinates loc = converter.toLocation(IP);
          String sessionURL = config.get("SessionPort")
              + "/logmining/?sessionid=" + entry.getKey() + "&sessionType="
              + outputType + "&requestType=" + inputType;
          session_count++;

          IndexRequest ir = new IndexRequest(config.get("indexName"),
              outputType).source(
                  jsonBuilder().startObject().field("SessionID", entry.getKey())
                      .field("SessionURL", sessionURL)
                      .field("Request_count", entry.getDocCount())
                      .field("Duration", duration)
                      .field("Number of Keywords", keywords_num)
                      .field("Time", min).field("End_time", max)
                      .field("searchDataListRequest_count",
                          searchDataListRequest_count)
                      .field("searchDataListRequest_byKeywords_count",
                          searchDataListRequest_byKeywords_count)
                      .field("searchDataRequest_count", searchDataRequest_count)
                      .field("keywords",
                          es.customAnalyzing(config.get("indexName"), keywords))
                      .field("views", views).field("downloads", downloads)
                      .field("request_rate", request_rate).field("Comments", "")
                      .field("Validation", 0).field("Produceby", 0)
                      .field("Correlation", 0).field("IP", IP)
                      // .field("Coordinates", loc.latlon)
                      .endObject());

          es.bulkProcessor.add(ir);
        }
      }
    }

    System.out.println("Session count:" + Integer.toString(session_count));
    es.destroyBulkProcessor();
  }

  public String findDataset(String request) {
    String pattern1 = "/dataset/";
    String pattern2;
    if (request.contains("?")) {
      pattern2 = "?";
    } else {
      pattern2 = " ";
    }

    Pattern p = Pattern
        .compile(Pattern.quote(pattern1) + "(.*?)" + Pattern.quote(pattern2));
    Matcher m = p.matcher(request);
    if (m.find()) {
      return m.group(1);
    }
    return null;
  }

  @Override
  public Object execute(Object o) {
    // TODO Auto-generated method stub
    return null;
  }

}
