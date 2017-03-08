package gov.nasa.jpl.mudrod.recommendation.structure;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import gov.nasa.jpl.mudrod.utils.LabeledRowMatrix;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import gov.nasa.jpl.mudrod.driver.ESDriver;
import gov.nasa.jpl.mudrod.driver.SparkDriver;
import gov.nasa.jpl.mudrod.utils.MatrixUtil;
import scala.Tuple2;

public class MetadataOpt implements Serializable {

  private String indexName;
  private String metadataType;
  private List<String> variables;

  public static final String SPLIT_BLANK = " ";
  public static final String SPLIT_COMMA = ",";

  public MetadataOpt(Properties props) {
    indexName = props.getProperty("indexName");
    metadataType = props.getProperty("recom_metadataType");

    variables = new ArrayList<String>();
    variables.add("DatasetParameter-Term");
    variables.add("DatasetParameter-Variable");
    variables.add("Dataset-Description");
    variables.add("Dataset-LongName");
  }

  public JavaPairRDD<String, String> loadAll(ESDriver es, SparkDriver spark)
      throws Exception {
    List<Tuple2<String, String>> datasetsTokens = this.loadMetadataFromES(es,
        variables);
    return this.parallizeData(spark, datasetsTokens);
  }

  public JavaPairRDD<String, String> loadAll(ESDriver es, SparkDriver spark,
      List<String> variables) throws Exception {
    List<Tuple2<String, String>> datasetsTokens = this.loadMetadataFromES(es,
        variables);
    return this.parallizeData(spark, datasetsTokens);
  }

  private JavaPairRDD<String, String> parallizeData(SparkDriver spark,
      List<Tuple2<String, String>> datasetContent) {

    JavaRDD<Tuple2<String, String>> datasetContentRDD = spark.sc
        .parallelize(datasetContent);

    JavaPairRDD<String, String> datasetsContentPairRDD = datasetContentRDD
        .mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
          @Override
          public Tuple2<String, String> call(Tuple2<String, String> term)
              throws Exception {
            return term;
          }
        });

    return datasetsContentPairRDD;
  }

  public JavaPairRDD<String, List<String>> tokenizeData(
      JavaPairRDD<String, String> datasetsContentRDD, String splitter)
      throws Exception {

    JavaPairRDD<String, List<String>> datasetTokensRDD = datasetsContentRDD
        .mapToPair(
            new PairFunction<Tuple2<String, String>, String, List<String>>() {
              @Override
              public Tuple2<String, List<String>> call(
                  Tuple2<String, String> arg) throws Exception {
                String content = arg._2;
                List<String> tokens = getTokens(content, splitter);

                return new Tuple2<String, List<String>>(arg._1, tokens);
              }
            });

    return datasetTokensRDD;
  }

  public List<String> getTokens(String str, String splitter) throws Exception {
    /* str = str.replaceAll("\\[", "").replaceAll("\\]", "").replace("\\(", "")
        .replace("\\)", "");*/
    String[] tokens = null;
    if (splitter.equals(SPLIT_BLANK)) {
      // str = str.replaceAll(",", " ").replaceAll(".", " ");
      tokens = str.split(" ");
    } else if (splitter.equals(SPLIT_COMMA)) {
      tokens = str.split(",");
    }
    List<String> list = java.util.Arrays.asList(tokens);
    return list;
  }

  public List<Tuple2<String, String>> loadMetadataFromES(ESDriver es,
      List<String> variables) throws Exception {

    SearchResponse scrollResp = es.getClient().prepareSearch(indexName)
        .setTypes(metadataType).setQuery(QueryBuilders.matchAllQuery())
        .setScroll(new TimeValue(60000)).setSize(100).execute().actionGet();

    List<Tuple2<String, String>> datasetsTokens = new ArrayList<Tuple2<String, String>>();
    while (true) {

      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> result = hit.getSource();
        String shortName = (String) result.get("Dataset-ShortName");

        String filedStr = "";
        int size = variables.size();
        for (int i = 0; i < size; i++) {
          String filed = variables.get(i);
          Object filedValue = result.get(filed);

          if (filedValue != null) {
            filedStr = es.customAnalyzing(indexName, filedValue.toString());
          }
        }

        datasetsTokens.add(new Tuple2<String, String>(shortName, filedStr));
      }

      scrollResp = es.getClient().prepareSearchScroll(scrollResp.getScrollId())
          .setScroll(new TimeValue(600000)).execute().actionGet();
      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }
    }

    return datasetsTokens;
  }

  public LabeledRowMatrix TFIDFTokens(
      JavaPairRDD<String, List<String>> datasetTokensRDD, SparkDriver spark) {

    LabeledRowMatrix labelMatrix = MatrixUtil
        .createDocWordMatrix(datasetTokensRDD, spark.sc);

    RowMatrix docwordMatrix = labelMatrix.rowMatrix;

    RowMatrix docwordTFIDFMatrix = MatrixUtil.createTFIDFMatrix(docwordMatrix,
        spark.sc);

    labelMatrix.rowMatrix = docwordTFIDFMatrix;

    return labelMatrix;
  }

  public LinkedHashMap<Object, Object> sortMapByValue(HashMap<?, ?> passedMap) {
    List<?> mapKeys = new ArrayList<Object>(passedMap.keySet());
    List<?> mapValues = new ArrayList<Object>(passedMap.values());
    Collections.sort(mapValues, Collections.reverseOrder());
    Collections.sort(mapKeys, Collections.reverseOrder());

    LinkedHashMap<Object, Object> sortedMap = new LinkedHashMap<Object, Object>();

    Iterator<?> valueIt = mapValues.iterator();
    while (valueIt.hasNext()) {
      Object val = valueIt.next();
      Iterator<?> keyIt = mapKeys.iterator();

      while (keyIt.hasNext()) {
        Object key = keyIt.next();
        String comp1 = passedMap.get(key).toString();
        String comp2 = val.toString();

        if (comp1.equals(comp2)) {
          passedMap.remove(key);
          mapKeys.remove(key);
          sortedMap.put(key, val);
          break;
        }

      }

    }
    return sortedMap;
  }

}
