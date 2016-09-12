package esiptestbed.mudrod.recommendation.structure;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.utils.LinkageTriple;
import esiptestbed.mudrod.utils.MatrixUtil;
import scala.Tuple2;

/**
 * Calculate metadata similarity from matrix
 */
public class ItemSimCalculator implements Serializable {

  // index name
  private String indexName;
  // type name of metadata
  private String metadataType;

  /**
   * Creates a new instance of ItemSimCalculator.
   *
   * @param props
   *          the Mudrod configuration
   */
  public ItemSimCalculator(Properties props) {
    indexName = props.getProperty("indexName");
    metadataType = props.getProperty("recom_metadataType");
  }

  /**
   * filter out-of-data metadata
   *
   * @param es
   *          the Elasticsearch drive
   * @param userDatasetsRDD
   *          dataset extracted from session
   * @return filtered session datasets
   */
  public JavaPairRDD<String, List<String>> filterData(ESDriver es,
      JavaPairRDD<String, List<String>> userDatasetsRDD) {
    Map<String, String> nameMap = this.getMetadataNameMap(es);

    JavaPairRDD<String, List<String>> filterUserDatasetsRDD = userDatasetsRDD
        .mapToPair(
            new PairFunction<Tuple2<String, List<String>>, String, List<String>>() {
              public Tuple2<String, Integer> call(String s) {
                String[] sarray = s.split(",");
                Double rate = Double.parseDouble(sarray[2]);
                return new Tuple2<String, Integer>(sarray[0] + "," + sarray[1],
                    rate.intValue());
              }

              @Override
              public Tuple2<String, List<String>> call(
                  Tuple2<String, List<String>> arg0) throws Exception {
                List<String> oriDatasets = arg0._2;
                List<String> newDatasets = new ArrayList<String>();
                int size = oriDatasets.size();
                for (int i = 0; i < size; i++) {
                  String name = oriDatasets.get(i);
                  if (nameMap.containsKey(name)) {
                    newDatasets.add(nameMap.get(name));
                  }
                }
                return new Tuple2<String, List<String>>(arg0._1, newDatasets);
              }
            });

    return filterUserDatasetsRDD;
  }

  /**
   * getMetadataNameMap: Get current metadata names
   *
   * @param es
   *          the elasticsearch client
   * @return a map from lower case metadata name to original metadata name
   */
  private Map<String, String> getMetadataNameMap(ESDriver es) {

    Map<String, String> shortnameMap = new HashMap<String, String>();
    SearchResponse scrollResp = es.getClient().prepareSearch(indexName)
        .setTypes(metadataType).setScroll(new TimeValue(60000))
        .setQuery(QueryBuilders.matchAllQuery()).setSize(100).execute()
        .actionGet();
    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> metadata = hit.getSource();
        String shortName = (String) metadata.get("Dataset-ShortName");
        shortnameMap.put(shortName.toLowerCase(), shortName);
      }

      scrollResp = es.getClient().prepareSearchScroll(scrollResp.getScrollId())
          .setScroll(new TimeValue(600000)).execute().actionGet();
      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }
    }

    return shortnameMap;
  }

  /**
   * Calculate metadata similarity from session coocurrence
   *
   * @param spark
   *          spark client
   * @param CSV_fileName
   *          matrix csv file name
   * @param skipRow
   *          the number of rows should be skipped from the first row
   * @return triple list, each one stands for the similarity between two
   *         datasets
   */
  public List<LinkageTriple> CalItemSimfromMatrix(SparkDriver spark,
      String CSV_fileName, int skipRow) {

    JavaPairRDD<String, Vector> importRDD = MatrixUtil.loadVectorFromCSV(spark,
        CSV_fileName, skipRow);

    IndexedRowMatrix indexedMatrix = MatrixUtil
        .buildIndexRowMatrix(importRDD.values());
    RowMatrix transposeMatrix = MatrixUtil.transposeMatrix(indexedMatrix);

    JavaRDD<Tuple2<String, Vector>> importRDD1 = importRDD
        .map(f -> new Tuple2<String, Vector>(f._1, f._2));
    JavaPairRDD<Tuple2<String, Vector>, Tuple2<String, Vector>> cartesianRDD = importRDD1
        .cartesian(importRDD1);

    JavaRDD<LinkageTriple> tripleRDD = cartesianRDD.map(
        new Function<Tuple2<Tuple2<String, Vector>, Tuple2<String, Vector>>, LinkageTriple>() {
          @Override
          public LinkageTriple call(
              Tuple2<Tuple2<String, Vector>, Tuple2<String, Vector>> arg) {
            String keyA = arg._1._1;
            String keyB = arg._2._1;

            if (keyA.equals(keyB)) {
              return null;
            }

            Vector vecA = arg._1._2;
            Vector vecB = arg._2._2;
            Double weight = itemSim(vecA, vecB);
            LinkageTriple triple = new LinkageTriple();
            triple.keyA = keyA;
            triple.keyB = keyB;
            triple.weight = weight;
            return triple;
          }
        }).filter(new Function<LinkageTriple, Boolean>() {
          @Override
          public Boolean call(LinkageTriple arg0) throws Exception {
            if (arg0 == null) {
              return false;
            }
            return true;
          }
        });

    return tripleRDD.collect();
  }

  /**
   * calculate similarity between vectors
   *
   * @param vecA
   * @param vecB
   * @return similarity between two vectors
   */
  private double itemSim(Vector vecA, Vector vecB) {
    double[] arrA = vecA.toArray();
    double[] arrB = vecB.toArray();

    int viewA = 0;
    int viewB = 0;
    int viewAB = 0;

    int arrsize = arrA.length;
    for (int i = 0; i < arrsize; i++) {
      if (arrA[i] > 0) {
        viewA++;
      }

      if (arrB[i] > 0) {
        viewB++;
      }

      if (arrB[i] > 0 && arrA[i] > 0) {
        viewAB++;
      }
    }

    double weight = viewAB / (Math.sqrt(viewA) * Math.sqrt(viewB));

    return weight;
  }
}
