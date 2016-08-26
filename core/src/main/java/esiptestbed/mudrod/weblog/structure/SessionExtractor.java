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
import java.util.Properties;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.main.MudrodConstants;
import scala.Tuple2;

/**
 * ClassName: SessionExtractor Function: Extract sessions details from
 * reconstructed sessions.
 *
 * @author Yun
 *
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
  public JavaRDD<ClickStream> extractClickStreamFromES(
      Properties props, ESDriver es, SparkDriver spark) {
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

            return new Tuple2<>(click.getViewDataset(),
                query);
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
    SearchResponse sr = es.getClient().prepareSearch(props.getProperty(MudrodConstants.ES_INDEX_NAME))
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
}
