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
package gov.nasa.jpl.mudrod.weblog.structure;

import gov.nasa.jpl.mudrod.driver.ESDriver;
import gov.nasa.jpl.mudrod.driver.SparkDriver;
import gov.nasa.jpl.mudrod.main.MudrodConstants;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * ClassName: SessionExtractor Function: Extract sessions details from
 * reconstructed sessions.
 */
public class SessionExtractor implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(SessionExtractor.class);

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  public SessionExtractor() {
    // default constructor
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
   * @return clickstream list in JavaRDD format {@link ClickStream}
   */
  public JavaRDD<ClickStream> extractClickStreamFromES(Properties props, ESDriver es, SparkDriver spark) {
    switch (props.getProperty(MudrodConstants.PROCESS_TYPE)) {
      case "sequential":
        List<ClickStream> queryList = this.getClickStreamList(props, es);
        return spark.sc.parallelize(queryList);
      case "parallel":
        return getClickStreamListInParallel(props, spark, es);
      default:
      LOG.error("Error finding processing type for '{}'. Please check your config.xml.", props.getProperty(MudrodConstants.PROCESS_TYPE));
    }
    return null;
  }

  /**
   * getClickStreamList:Extract click streams from logs stored in Elasticsearch.
   *
   * @param props
   *          the Mudrod configuration
   * @param es
   *          the Elasticsearch driver
   * @return clickstream list {@link ClickStream}
   */
  protected List<ClickStream> getClickStreamList(Properties props, ESDriver es) {
    List<String> logIndexList = es.getIndexListWithPrefix(props.getProperty(MudrodConstants.LOG_INDEX));

    List<ClickStream> result = new ArrayList<>();
    for (int n = 0; n < logIndexList.size(); n++) {
      String logIndex = logIndexList.get(n);
      List<String> sessionIdList;
      try {
        sessionIdList = this.getSessions(props, es, logIndex);
        Session session = new Session(props, es);
        int sessionNum = sessionIdList.size();
        for (int i = 0; i < sessionNum; i++) {
          String[] sArr = sessionIdList.get(i).split(",");
          List<ClickStream> datas = session.getClickStreamList(sArr[1], sArr[2], sArr[0]);
          result.addAll(datas);
        }
      } catch (Exception e) {
        LOG.error("Error during extraction of Clickstreams from log index. {}", e);
      }
    }

    return result;
  }

  protected JavaRDD<ClickStream> getClickStreamListInParallel(Properties props, SparkDriver spark, ESDriver es) {

    List<String> logIndexList = es.getIndexListWithPrefix(props.getProperty(MudrodConstants.LOG_INDEX));

    LOG.info("Retrieved {}", logIndexList.toString());

    List<String> sessionIdList = new ArrayList<>();
    for (int n = 0; n < logIndexList.size(); n++) {
      String logIndex = logIndexList.get(n);
      List<String> tmpsessionList = this.getSessions(props, es, logIndex);
      sessionIdList.addAll(tmpsessionList);
    }

    JavaRDD<String> sessionRDD = spark.sc.parallelize(sessionIdList, 16);

    JavaRDD<ClickStream> clickStreamRDD = sessionRDD.mapPartitions(new FlatMapFunction<Iterator<String>, ClickStream>() {
      /**
       *
       */
      private static final long serialVersionUID = 1L;

      @Override
      public Iterator<ClickStream> call(Iterator<String> arg0) throws Exception {
        ESDriver tmpES = new ESDriver(props);
        tmpES.createBulkProcessor();

        Session session = new Session(props, tmpES);
        List<ClickStream> clickstreams = new ArrayList<>();
        while (arg0.hasNext()) {
          String s = arg0.next();
          String[] sArr = s.split(",");
          List<ClickStream> clicks = session.getClickStreamList(sArr[1], sArr[2], sArr[0]);
          clickstreams.addAll(clicks);
        }
        tmpES.destroyBulkProcessor();
        tmpES.close();
        return clickstreams.iterator();
      }
    });

    LOG.info("Clickstream number: {}", clickStreamRDD.count());

    return clickStreamRDD;
  }

  // This function is reserved and not being used for now

  /**
   * loadClickStremFromTxt:Load click stream form txt file
   *
   * @param clickthroughFile
   *          txt file
   * @param sc
   *          the spark context
   * @return clickstream list in JavaRDD format {@link ClickStream}
   */
  public JavaRDD<ClickStream> loadClickStremFromTxt(String clickthroughFile, JavaSparkContext sc) {
    return sc.textFile(clickthroughFile).flatMap(new FlatMapFunction<String, ClickStream>() {
      /**
       *
       */
      private static final long serialVersionUID = 1L;

      @SuppressWarnings("unchecked")
      @Override
      public Iterator<ClickStream> call(String line) throws Exception {
        List<ClickStream> clickthroughs = (List<ClickStream>) ClickStream.parseFromTextLine(line);
        return (Iterator<ClickStream>) clickthroughs;
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
  public JavaPairRDD<String, List<String>> bulidDataQueryRDD(JavaRDD<ClickStream> clickstreamRDD, int downloadWeight) {
    return clickstreamRDD.mapToPair(new PairFunction<ClickStream, String, List<String>>() {
      /**
       *
       */
      private static final long serialVersionUID = 1L;

      @Override
      public Tuple2<String, List<String>> call(ClickStream click) throws Exception {
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
    }).reduceByKey(new Function2<List<String>, List<String>, List<String>>() {
      /**
       *
       */
      private static final long serialVersionUID = 1L;

      @Override
      public List<String> call(List<String> v1, List<String> v2) throws Exception {
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
   *          the Elasticsearch driver
   * @param logIndex
   *          a log index name
   * @return list of session names
   */
  protected List<String> getSessions(Properties props, ESDriver es, String logIndex) {

    String cleanupPrefix = props.getProperty(MudrodConstants.CLEANUP_TYPE_PREFIX);
    String sessionStatPrefix = props.getProperty(MudrodConstants.SESSION_STATS_PREFIX);

    List<String> sessions = new ArrayList<>();
    SearchResponse scrollResp = es.getClient().prepareSearch(logIndex).setTypes(sessionStatPrefix).setScroll(new TimeValue(60000)).setQuery(QueryBuilders.matchAllQuery()).setSize(100).execute()
            .actionGet();
    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> session = hit.getSource();
        String sessionID = (String) session.get("SessionID");
        sessions.add(sessionID + "," + logIndex + "," + cleanupPrefix);
      }

      scrollResp = es.getClient().prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }
    }

    return sessions;
  }

  public JavaPairRDD<String, Double> bulidUserItermRDD(JavaRDD<ClickStream> clickstreamRDD) {
    return clickstreamRDD.mapToPair(new PairFunction<ClickStream, String, Double>() {
      /**
       *
       */
      private static final long serialVersionUID = 1L;

      @Override
      public Tuple2<String, Double> call(ClickStream click) throws Exception {
        double rate = 1;
        boolean download = click.isDownload();
        if (download) {
          rate = 2;
        }

        String sessionID = click.getSessionID();
        String user = sessionID.split("@")[0];

        return new Tuple2<>(user + "," + click.getViewDataset(), rate);
      }
    }).reduceByKey(new Function2<Double, Double, Double>() {
      /**
       *
       */
      private static final long serialVersionUID = 1L;

      @Override
      public Double call(Double v1, Double v2) throws Exception {
        return v1 >= v2 ? v1 : v2;

      }
    });
  }

  public JavaPairRDD<String, Double> bulidSessionItermRDD(JavaRDD<ClickStream> clickstreamRDD) {
    JavaPairRDD<String, String> sessionItemRDD = clickstreamRDD.mapToPair(new PairFunction<ClickStream, String, String>() {
      /**
       *
       */
      private static final long serialVersionUID = 1L;

      @Override
      public Tuple2<String, String> call(ClickStream click) throws Exception {

        String sessionID = click.getSessionID();
        return new Tuple2<>(sessionID, click.getViewDataset());
      }
    }).distinct();

    // remove some sessions
    JavaPairRDD<String, Double> sessionItemNumRDD = sessionItemRDD.keys().mapToPair(new PairFunction<String, String, Double>() {
      /**
       *
       */
      private static final long serialVersionUID = 1L;

      @Override
      public Tuple2<String, Double> call(String item) throws Exception {
        return new Tuple2<>(item, 1.0);
      }
    }).reduceByKey(new Function2<Double, Double, Double>() {
      /**
       *
       */
      private static final long serialVersionUID = 1L;

      @Override
      public Double call(Double v1, Double v2) throws Exception {
        return v1 + v2;
      }
    }).filter(new Function<Tuple2<String, Double>, Boolean>() {
      /**
       *
       */
      private static final long serialVersionUID = 1L;

      @Override
      public Boolean call(Tuple2<String, Double> arg0) throws Exception {
        Boolean b = true;
        if (arg0._2 < 2) {
          b = false;
        }
        return b;
      }
    });

    return sessionItemNumRDD.leftOuterJoin(sessionItemRDD).mapToPair(new PairFunction<Tuple2<String, Tuple2<Double, Optional<String>>>, String, Double>() {
      /**
       *
       */
      private static final long serialVersionUID = 1L;

      @Override
      public Tuple2<String, Double> call(Tuple2<String, Tuple2<Double, Optional<String>>> arg0) throws Exception {

        Tuple2<Double, Optional<String>> test = arg0._2;
        Optional<String> optStr = test._2;
        String item = "";
        if (optStr.isPresent()) {
          item = optStr.get();
        }
        return new Tuple2<>(arg0._1 + "," + item, 1.0);
      }

    });
  }

  public JavaPairRDD<String, List<String>> bulidSessionDatasetRDD(Properties props, ESDriver es, SparkDriver spark) {

    List<String> result = new ArrayList<>();
    List<String> logIndexList = es.getIndexListWithPrefix(props.getProperty(MudrodConstants.LOG_INDEX));
    for (int n = 0; n < logIndexList.size(); n++) {
      String logIndex = logIndexList.get(n);
      SearchResponse scrollResp = es.getClient().prepareSearch(logIndex).setTypes(props.getProperty(MudrodConstants.SESSION_STATS_PREFIX)).setScroll(new TimeValue(60000)).setQuery(QueryBuilders.matchAllQuery())
              .setSize(100).execute().actionGet();
      while (true) {
        for (SearchHit hit : scrollResp.getHits().getHits()) {
          Map<String, Object> session = hit.getSource();
          String sessionID = (String) session.get("SessionID");
          String views = (String) session.get("views");
          if (views != null && !"".equals(views)) {
            String sessionItems = sessionID + ":" + views;
            result.add(sessionItems);
          }
        }

        scrollResp = es.getClient().prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
        if (scrollResp.getHits().getHits().length == 0) {
          break;
        }
      }
    }

    JavaRDD<String> sessionRDD = spark.sc.parallelize(result);

    return sessionRDD.mapToPair(new PairFunction<String, String, List<String>>() {
      private static final long serialVersionUID = 1L;

      @Override
      public Tuple2<String, List<String>> call(String sessionitem) throws Exception {
        String[] splits = sessionitem.split(":");
        String sessionId = splits[0];
        List<String> itemList = new ArrayList<>();

        String items = splits[1];
        String[] itemArr = items.split(",");
        int size = itemArr.length;
        for (int i = 0; i < size; i++) {
          String item = itemArr[i];
          if (!itemList.contains(item))
            itemList.add(itemArr[i]);
        }

        return new Tuple2<>(sessionId, itemList);
      }
    });
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
   * @return clickstream list in JavaRDD format {@link ClickStream}
   */
  public JavaRDD<RankingTrainData> extractRankingTrainData(Properties props, ESDriver es, SparkDriver spark) {

    List<RankingTrainData> queryList = this.extractRankingTrainData(props, es);
    return spark.sc.parallelize(queryList);

  }

  /**
   * getClickStreamList:Extract click streams from logs stored in Elasticsearch.
   *
   * @param props
   *          the Mudrod configuration
   * @param es
   *          the Elasticsearch driver
   * @return clickstream list {@link ClickStream}
   */
  protected List<RankingTrainData> extractRankingTrainData(Properties props, ESDriver es) {
    List<String> logIndexList = es.getIndexListWithPrefix(props.getProperty(MudrodConstants.LOG_INDEX));

    LOG.info(logIndexList.toString());

    List<RankingTrainData> result = new ArrayList<>();
    for (int n = 0; n < logIndexList.size(); n++) {
      String logIndex = logIndexList.get(n);
      List<String> sessionIdList;
      try {
        sessionIdList = this.getSessions(props, es, logIndex);
        Session session = new Session(props, es);
        int sessionNum = sessionIdList.size();
        for (int i = 0; i < sessionNum; i++) {
          String[] sArr = sessionIdList.get(i).split(",");
          List<RankingTrainData> datas = session.getRankingTrainData(sArr[1], sArr[2], sArr[0]);
          result.addAll(datas);
        }
      } catch (Exception e) {
        LOG.error("Error which extracting ranking train data: {}", e);
      }
    }

    return result;
  }

  protected JavaRDD<RankingTrainData> extractRankingTrainDataInParallel(Properties props, SparkDriver spark, ESDriver es) {

    List<String> logIndexList = es.getIndexListWithPrefix(props.getProperty(MudrodConstants.LOG_INDEX));

    LOG.info(logIndexList.toString());

    List<String> sessionIdList = new ArrayList<>();
    for (int n = 0; n < logIndexList.size(); n++) {
      String logIndex = logIndexList.get(n);
      List<String> tmpsessionList = this.getSessions(props, es, logIndex);
      sessionIdList.addAll(tmpsessionList);
    }

    JavaRDD<String> sessionRDD = spark.sc.parallelize(sessionIdList, 16);

    JavaRDD<RankingTrainData> clickStreamRDD = sessionRDD.mapPartitions(new FlatMapFunction<Iterator<String>, RankingTrainData>() {
      /**
       *
       */
      private static final long serialVersionUID = 1L;

      @Override
      public Iterator<RankingTrainData> call(Iterator<String> arg0) throws Exception {
        ESDriver tmpES = new ESDriver(props);
        tmpES.createBulkProcessor();

        Session session = new Session(props, tmpES);
        List<RankingTrainData> clickstreams = new ArrayList<>();
        while (arg0.hasNext()) {
          String s = arg0.next();
          String[] sArr = s.split(",");
          List<RankingTrainData> clicks = session.getRankingTrainData(sArr[1], sArr[2], sArr[0]);
          clickstreams.addAll(clicks);
        }
        tmpES.destroyBulkProcessor();
        tmpES.close();
        return clickstreams.iterator();
      }
    });

    LOG.info("Clickstream number: {}", clickStreamRDD.count());

    return clickStreamRDD;
  }

}
