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
package esiptestbed.mudrod.weblog.structure;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import com.google.common.base.Optional;

import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.main.MudrodConstants;
import scala.Tuple2;

/**
 * ClassName: SessionExtractor Function: Extract sessions details from
 * reconstructed sessions.
 */
public class SessionExtractor implements Serializable {

  public SessionExtractor() {
  }

  /**
   * extractClickStreamFromES:Extract click streams from logs stored in
   * Elasticsearch
   *
   * @param props
   *          the Mudrod configuration
   * @param es
   *          the Elasticsearch drive
   * @param spark
   *          the spark driver
   * @return clickstream list in JavaRDD format
   *         {@link esiptestbed.mudrod.weblog.structure.ClickStream}
   */
  public JavaRDD<ClickStream> extractClickStreamFromES(Properties props,
      ESDriver es, SparkDriver spark) {
    List<ClickStream> queryList = null;
    try {
      queryList = this.getClickStreamList(props, es);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return spark.sc.parallelize(queryList);
  }

  /**
   * getClickStreamList:Extract click streams from logs stored in Elasticsearch.
   *
   * @param props
   *          the Mudrod configuration
   * @param es
   *          the Elasticsearch driver
   * @return clickstream list
   *         {@link esiptestbed.mudrod.weblog.structure.ClickStream}
   */
  protected List<ClickStream> getClickStreamList(Properties props,
      ESDriver es) {
    ArrayList<String> cleanupTypeList = es.getTypeListWithPrefix(
        props.getProperty(MudrodConstants.ES_INDEX_NAME),
        props.getProperty(MudrodConstants.CLEANUP_TYPE_PREFIX));
    List<ClickStream> result = new ArrayList<>();
    for (int n = 0; n < cleanupTypeList.size(); n++) {
      String cleanupType = cleanupTypeList.get(n);
      List<String> sessionIdList;
      try {
        sessionIdList = this.getSessions(props, es, cleanupType);
        Session session = new Session(props, es);
        int sessionNum = sessionIdList.size();
        for (int i = 0; i < sessionNum; i++) {
          List<ClickStream> datas = session.getClickStreamList(cleanupType,
              sessionIdList.get(i));
          result.addAll(datas);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    return result;
  }

  // This function is reserved and not being used for now
  /**
   * loadClickStremFromTxt:Load click stream form txt file
   *
   * @param clickthroughFile
   *          txt file
   * @param sc
   *          the spark context
   * @return clickstream list in JavaRDD format
   *         {@link esiptestbed.mudrod.weblog.structure.ClickStream}
   */
  public JavaRDD<ClickStream> loadClickStremFromTxt(String clickthroughFile,
      JavaSparkContext sc) {
    return sc.textFile(clickthroughFile)
        .flatMap(new FlatMapFunction<String, ClickStream>() {
          /**
           *
           */
          private static final long serialVersionUID = 1L;

          @Override
          public Iterable<ClickStream> call(String line) throws Exception {
            List<ClickStream> clickthroughs = (List<ClickStream>) ClickStream
                .parseFromTextLine(line);
            return clickthroughs;
          }
        });
  }

  /**
   * bulidDataQueryRDD: convert click stream list to data set queries pairs.
   *
   * @param clickstreamRDD:
   *          click stream data
   * @param downloadWeight:
   *          weight of download behavior
   * @return JavaPairRDD, key is short name of data set, and values are queries
   */
  public JavaPairRDD<String, List<String>> bulidDataQueryRDD(
      JavaRDD<ClickStream> clickstreamRDD, int downloadWeight) {
    return clickstreamRDD
        .mapToPair(new PairFunction<ClickStream, String, List<String>>() {
          /**
           *
           */
          private static final long serialVersionUID = 1L;

          @Override
          public Tuple2<String, List<String>> call(ClickStream click)
              throws Exception {
            List<String> query = new ArrayList<>();
            // important! download behavior is given higher weights
            // than viewing
            // behavior
            boolean download = click.isDownload();
            int weight = 1;
            if (download) {
              weight = downloadWeight;
            }
            for (int i = 0; i < weight; i++) {
              query.add(click.getKeyWords());
            }

            return new Tuple2<>(click.getViewDataset(), query);
          }
        })
        .reduceByKey(new Function2<List<String>, List<String>, List<String>>() {
          /**
           *
           */
          private static final long serialVersionUID = 1L;

          @Override
          public List<String> call(List<String> v1, List<String> v2)
              throws Exception {
            List<String> list = new ArrayList<>();
            list.addAll(v1);
            list.addAll(v2);
            return list;
          }
        });
  }

  /**
   * getSessions: Get sessions from logs
   *
   * @param props
   *          the Mudrod configuration
   * @param es
   *          the Elasticsearch drive
   * @param cleanupType
   *          session type name
   * @return list of session names
   */
  protected List<String> getSessions(Properties props, ESDriver es,
      String cleanupType) {
    List<String> sessionIDList = new ArrayList<>();
    SearchResponse sr = es.getClient()
        .prepareSearch(props.getProperty(MudrodConstants.ES_INDEX_NAME))
        .setTypes(cleanupType).setQuery(QueryBuilders.matchAllQuery())
        .setSize(0)
        .addAggregation(
            AggregationBuilders.terms("Sessions").field("SessionID").size(0))
        .execute().actionGet();
    Terms sessions = sr.getAggregations().get("Sessions");
    for (Terms.Bucket entry : sessions.getBuckets()) {
      sessionIDList.add(entry.getKey().toString());
    }
    return sessionIDList;
  }

  public JavaPairRDD<String, Double> bulidUserItermRDD(
      JavaRDD<ClickStream> clickstreamRDD) {
    JavaPairRDD<String, Double> useritem_rateRDD = clickstreamRDD
        .mapToPair(new PairFunction<ClickStream, String, Double>() {
          @Override
          public Tuple2<String, Double> call(ClickStream click)
              throws Exception {
            double rate = 1;
            boolean download = click.isDownload();
            if (download) {
              rate = 2;
            }

            String sessionID = click.getSessionID();
            String user = sessionID.split("@")[0];

            return new Tuple2<String, Double>(
                user + "," + click.getViewDataset(), rate);
          }
        }).reduceByKey(new Function2<Double, Double, Double>() {
          @Override
          public Double call(Double v1, Double v2) throws Exception {
            // TODO Auto-generated method stub
            return v1 >= v2 ? v1 : v2;

          }
        });

    return useritem_rateRDD;
  }

  public JavaPairRDD<String, Double> bulidSessionItermRDD(
      JavaRDD<ClickStream> clickstreamRDD, int filterOpt) {
    JavaPairRDD<String, String> sessionItemRDD = clickstreamRDD
        .mapToPair(new PairFunction<ClickStream, String, String>() {
          @Override
          public Tuple2<String, String> call(ClickStream click)
              throws Exception {

            String sessionID = click.getSessionID();
            return new Tuple2<String, String>(sessionID,
                click.getViewDataset());
          }
        }).distinct();

    // remove some sessions
    JavaPairRDD<String, Double> sessionItemNumRDD = sessionItemRDD.keys()
        .mapToPair(new PairFunction<String, String, Double>() {
          @Override
          public Tuple2<String, Double> call(String item) throws Exception {
            return new Tuple2<String, Double>(item, 1.0);
          }
        }).reduceByKey(new Function2<Double, Double, Double>() {
          @Override
          public Double call(Double v1, Double v2) throws Exception {
            return v1 + v2;
          }
        }).filter(new Function<Tuple2<String, Double>, Boolean>() {
          @Override
          public Boolean call(Tuple2<String, Double> arg0) throws Exception {
            Boolean b = true;
            if (arg0._2 < 2) {
              b = false;
            }
            return b;
          }
        });

    JavaPairRDD<String, Double> filteredSessionItemRDD = sessionItemNumRDD
        .leftOuterJoin(sessionItemRDD).mapToPair(
            new PairFunction<Tuple2<String, Tuple2<Double, Optional<String>>>, String, Double>() {
              @Override
              public Tuple2<String, Double> call(
                  Tuple2<String, Tuple2<Double, Optional<String>>> arg0)
                  throws Exception {

                Tuple2<Double, Optional<String>> test = arg0._2;
                Optional<String> optStr = test._2;
                String item = "";
                if (optStr.isPresent()) {
                  item = optStr.get();
                }
                return new Tuple2<String, Double>(arg0._1 + "," + item, 1.0);
              }

            });

    return filteredSessionItemRDD;
  }

  public JavaPairRDD<String, List<String>> bulidSessionItermRDD(
      Properties props, ESDriver es, SparkDriver spark) {

    ArrayList<String> sessionstatic_typeList = es.getTypeListWithPrefix(
        props.getProperty("indexName"),
        props.getProperty("SessionStats_prefix"));
    List<String> result = new ArrayList<>();
    for (int n = 0; n < sessionstatic_typeList.size(); n++) {

      String staticType = sessionstatic_typeList.get(n);

      SearchResponse scrollResp = es.getClient()
          .prepareSearch(props.getProperty("indexName")).setTypes(staticType)
          .setScroll(new TimeValue(60000))
          .setQuery(QueryBuilders.matchAllQuery()).setSize(100).execute()
          .actionGet();
      while (true) {
        for (SearchHit hit : scrollResp.getHits().getHits()) {
          Map<String, Object> session = hit.getSource();
          String sessionID = (String) session.get("SessionID");
          String views = (String) session.get("views");
          if (views != null && !views.equals("")) {
            String sessionItems = sessionID + ":" + views;
            result.add(sessionItems);
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

    JavaRDD<String> sessionRDD = spark.sc.parallelize(result);

    JavaPairRDD<String, List<String>> sessionItemRDD = sessionRDD
        .mapToPair(new PairFunction<String, String, List<String>>() {
          @Override
          public Tuple2<String, List<String>> call(String sessionitem)
              throws Exception {
            String[] splits = sessionitem.split(":");
            String sessionId = splits[0];
            List<String> itemList = new ArrayList<String>();

            String items = splits[1];
            String[] itemArr = items.split(",");
            int size = itemArr.length;
            for (int i = 0; i < size; i++) {
              String item = itemArr[i];
              if (!itemList.contains(item))
                itemList.add(itemArr[i]);
            }

            return new Tuple2<String, List<String>>(sessionId, itemList);
          }
        });

    return sessionItemRDD;
  }

}
