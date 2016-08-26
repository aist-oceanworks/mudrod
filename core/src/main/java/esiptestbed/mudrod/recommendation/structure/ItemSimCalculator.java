package esiptestbed.mudrod.recommendation.structure;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
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

public class ItemSimCalculator implements Serializable {

  JavaPairRDD<String, Long> userIDRDD = null;
  JavaPairRDD<String, Long> itemIDRDD = null;

  private String indexName;
  private String metadataType;

  public ItemSimCalculator(Properties props) {
    // TODO Auto-generated constructor stub
    indexName = props.getProperty("indexName");
    metadataType = props.getProperty("recom_metadataType");
  }

  public JavaPairRDD<String, List<String>> prepareData(JavaSparkContext sc,
      String path) {

    // load data
    JavaRDD<String> data = sc.textFile(path)
        .map(new Function<String, String>() {
          @Override
          public String call(String s) {
            String line = s.substring(1, s.length() - 1);
            return line;
          }
        });

    // generate user-id, item-id
    JavaPairRDD<String, Integer> useritem_rateRDD = data
        .mapToPair(new PairFunction<String, String, Integer>() {
          @Override
          public Tuple2<String, Integer> call(String s) {
            String[] sarray = s.split(",");
            Double rate = Double.parseDouble(sarray[2]);
            return new Tuple2<String, Integer>(sarray[0] + "," + sarray[1],
                rate.intValue());
          }
        });

    JavaPairRDD<String, List<String>> testRDD = useritem_rateRDD.flatMapToPair(
        new PairFlatMapFunction<Tuple2<String, Integer>, String, List<String>>() {
          @Override
          public Iterable<Tuple2<String, List<String>>> call(
              Tuple2<String, Integer> arg0) throws Exception {
            // TODO Auto-generated method stub
            List<Tuple2<String, List<String>>> pairs = new ArrayList<Tuple2<String, List<String>>>();
            String words = arg0._1;
            int n = arg0._2;
            for (int i = 0; i < n; i++) {
              List<String> l = new ArrayList<String>();
              l.add(words.split(",")[1]);
              Tuple2<String, List<String>> worddoc = new Tuple2<String, List<String>>(
                  words.split(",")[0], l);
              pairs.add(worddoc);
            }
            return pairs;
          }
        })
        .reduceByKey(new Function2<List<String>, List<String>, List<String>>() {
          @Override
          public List<String> call(List<String> arg0, List<String> arg1)
              throws Exception {
            // TODO Auto-generated method stub
            List<String> newlist = new ArrayList<String>();
            newlist.addAll(arg0);
            newlist.addAll(arg1);
            return newlist;
          }
        });

    return testRDD;
  }

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
                // TODO Auto-generated method stub
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
            // TODO Auto-generated method stub
            if (arg0 == null) {
              return false;
            }
            return true;
          }
        });

    return tripleRDD.collect();
  }

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
