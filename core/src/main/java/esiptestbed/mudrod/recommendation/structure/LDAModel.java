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
package esiptestbed.mudrod.recommendation.structure;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.clustering.DistributedLDAModel;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.utils.LabeledRowMatrix;
import esiptestbed.mudrod.utils.MatrixUtil;
import scala.Tuple2;
import scala.Tuple3;

public class LDAModel implements Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private String indexName;
  private String metadataType;

  public List<String> variables;

  public LDAModel(Properties props) {
    indexName = props.getProperty("indexName");
    metadataType = props.getProperty("recom_metadataType");

    variables = new ArrayList<String>();
    variables.add("DatasetParameter-Term");
    variables.add("DatasetParameter-Variable");
    variables.add("Dataset-Description");
    variables.add("Dataset-LongName");
  }

  public JavaPairRDD<String, List<String>> loadData(ESDriver es,
      SparkDriver spark) throws Exception {

    List<Tuple2<String, List<String>>> datasetsTokens = this
        .loadMetadataFromES(es);

    JavaRDD<Tuple2<String, List<String>>> datasetsTokensRDD = spark.sc
        .parallelize(datasetsTokens);

    JavaPairRDD<String, List<String>> datasetsTokenPairRDD = datasetsTokensRDD
        .mapToPair(
            new PairFunction<Tuple2<String, List<String>>, String, List<String>>() {
              /**
               * 
               */
              private static final long serialVersionUID = 1L;

              @Override
              public Tuple2<String, List<String>> call(
                  Tuple2<String, List<String>> term) throws Exception {
                return term;
              }
            });

    return datasetsTokenPairRDD;
  }

  public List<Tuple2<String, List<String>>> loadMetadataFromES(ESDriver es)
      throws Exception {

    SearchResponse scrollResp = es.getClient().prepareSearch(indexName)
        .setTypes(metadataType).setQuery(QueryBuilders.matchAllQuery())
        .setScroll(new TimeValue(60000)).setSize(100).execute().actionGet();

    List<Tuple2<String, List<String>>> datasetsTokens = new ArrayList<Tuple2<String, List<String>>>();
    while (true) {

      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> result = hit.getSource();
        String shortName = (String) result.get("Dataset-ShortName");

        List<String> tokens = new ArrayList<String>();
        int size = variables.size();
        for (int i = 0; i < size; i++) {
          String filed = variables.get(i);
          Object filedValue = result.get(filed);

          if (filedValue != null) {
            String filedStr = es.customAnalyzing(indexName,
                filedValue.toString());

            List<String> filedTokenList = this.getTokens(filedStr);
            tokens.addAll(filedTokenList);
          }
        }

        datasetsTokens.add(new Tuple2<String, List<String>>(shortName, tokens));
      }

      scrollResp = es.getClient().prepareSearchScroll(scrollResp.getScrollId())
          .setScroll(new TimeValue(600000)).execute().actionGet();
      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }
    }

    return datasetsTokens;
  }

  public List<String> getTokens(String str) throws Exception {
    String[] splits = str.split(" ");
    List<String> list = java.util.Arrays.asList(splits);
    return list;
  }

  public LabeledRowMatrix getTFIDF(
      JavaPairRDD<String, List<String>> datasetTokensRDD, SparkDriver spark,
      String file) {

    LabeledRowMatrix labelMatrix = MatrixUtil
        .createDocWordMatrix(datasetTokensRDD, spark.sc);

    RowMatrix docwordMatrix = labelMatrix.wordDocMatrix;

    RowMatrix docwordTFIDFMatrix = MatrixUtil.createTFIDFMatrix(docwordMatrix,
        spark.sc);

    labelMatrix.wordDocMatrix = docwordTFIDFMatrix;

    return labelMatrix;
  }

  public LabeledRowMatrix getDocWordMatrix(
      JavaPairRDD<String, List<String>> datasetTokensRDD, SparkDriver spark,
      String file) {

    LabeledRowMatrix labelMatrix = MatrixUtil
        .createDocWordMatrix(datasetTokensRDD, spark.sc);

    return labelMatrix;
  }

  public LabeledRowMatrix getLDA(
      JavaPairRDD<String, List<String>> datasetTokensRDD, SparkDriver spark,
      String file) {

    LabeledRowMatrix labelMatrix = MatrixUtil
        .createWordDocMatrix(datasetTokensRDD, spark.sc);
    RowMatrix wordDocMatrix = labelMatrix.wordDocMatrix;

    IndexedRowMatrix indexedMatrix = MatrixUtil
        .buildIndexRowMatrix(wordDocMatrix.rows().toJavaRDD());
    RowMatrix docWordMatrix = MatrixUtil.transposeMatrix(indexedMatrix);
    JavaRDD<Vector> parsedData = docWordMatrix.rows().toJavaRDD();

    // Index documents with unique IDs
    JavaPairRDD<Long, Vector> corpus = JavaPairRDD
        .fromJavaRDD(parsedData.zipWithIndex()
            .map(new Function<Tuple2<Vector, Long>, Tuple2<Long, Vector>>() {
              /**
               * 
               */
              private static final long serialVersionUID = 1L;

              @Override
              public Tuple2<Long, Vector> call(Tuple2<Vector, Long> doc_id) {
                return doc_id.swap();
              }
            }));

    // Cluster the documents into three topics using LDA
    int topicnum = 50;
    org.apache.spark.mllib.clustering.LDAModel ldaModel = new LDA()
        .setK(topicnum).setDocConcentration(2).setTopicConcentration(2)
        .setMaxIterations(topicnum).setSeed(0L).setCheckpointInterval(10)
        .setOptimizer("em").run(corpus);

    this.DeleteFileFolder(file);

    ldaModel.save(spark.sc.sc(), file);

    DistributedLDAModel sameModel = DistributedLDAModel.load(spark.sc.sc(),
        file);

    JavaRDD<Tuple3<Object, int[], double[]>> metadataTopicProb = sameModel
        .topTopicsPerDocument(topicnum).toJavaRDD();

    JavaPairRDD<Long, Vector> metadata_id_vector = metadataTopicProb.mapToPair(
        new PairFunction<Tuple3<Object, int[], double[]>, Long, Vector>() {
          /**
           * 
           */
          private static final long serialVersionUID = 1L;

          @Override
          public Tuple2<Long, Vector> call(Tuple3<Object, int[], double[]> arg0)
              throws Exception {
            Long dataid = (Long) arg0._1();
            Vector vec = Vectors.dense(arg0._3());
            return new Tuple2<Long, Vector>(dataid, vec);
          }
        });

    JavaPairRDD<Long, String> id_datasetRDD = JavaPairRDD
        .fromJavaRDD(datasetTokensRDD.keys().zipWithIndex()
            .map(new Function<Tuple2<String, Long>, Tuple2<Long, String>>() {
              /**
               * 
               */
              private static final long serialVersionUID = 1L;

              @Override
              public Tuple2<Long, String> call(Tuple2<String, Long> doc_id) {
                return doc_id.swap();
              }
            }));

    /* JavaPairRDD<String, Vector> dataset_vectorRDD = id_datasetRDD
        .leftOuterJoin(metadata_id_vector).values().mapToPair(
            new PairFunction<Tuple2<String, Optional<Vector>>, String, Vector>() {
              *//**
                * 
                *//*
                  private static final long serialVersionUID = 1L;
                  
                  @Override
                  public Tuple2<String, Vector> call(
                   Tuple2<String, Optional<Vector>> arg0) throws Exception {
                  Optional<Vector> oVec = arg0._2;
                  Vector vec = null;
                  if (oVec.isPresent()) {
                   vec = oVec.get();
                  }
                  return new Tuple2<String, Vector>(arg0._1, vec);
                  }
                  
                  });
                  
                  LabeledRowMatrix lmatrix = new LabeledRowMatrix();
                  lmatrix.words = dataset_vectorRDD.keys().collect();
                  RowMatrix rowMatrix = new RowMatrix(dataset_vectorRDD.values().rdd());
                  lmatrix.wordDocMatrix = rowMatrix;
                  
                  List<String> topics = new ArrayList<String>();
                  for (int i = 0; i < topicnum; i++) {
                  topics.add("topic" + i);
                  }
                  
                  lmatrix.docs = topics;
                  
                  return lmatrix;*/

    return null;
  }

  /**
   * Method of sorting a map by value
   * @param passedMap input map
   * @return sorted map
   */
  public Map<String, Double> sortMapByValue(Map<String, Double> passedMap) {
    List<String> mapKeys = new ArrayList<>(passedMap.keySet());
    List<Double> mapValues = new ArrayList<>(passedMap.values());
    Collections.sort(mapValues, Collections.reverseOrder());
    Collections.sort(mapKeys, Collections.reverseOrder());

    LinkedHashMap<String, Double> sortedMap = new LinkedHashMap<>();

    Iterator<Double> valueIt = mapValues.iterator();
    while (valueIt.hasNext()) {
      Object val = valueIt.next();
      Iterator<String> keyIt = mapKeys.iterator();

      while (keyIt.hasNext()) {
        Object key = keyIt.next();
        String comp1 = passedMap.get(key).toString();
        String comp2 = val.toString();

        if (comp1.equals(comp2)) {
          passedMap.remove(key);
          mapKeys.remove(key);
          sortedMap.put((String) key, (Double) val);
          break;
        }

      }

    }
    return sortedMap;
  }

  public void DeleteFileFolder(String path) {
    File file = new File(path);
    if (file.exists()) {
      do {
        delete(file);
      } while (file.exists());
    } else {
    }

  }

  private void delete(File file) {
    if (file.isDirectory()) {
      String fileList[] = file.list();
      if (fileList.length == 0) {
        file.delete();
      } else {
        int size = fileList.length;
        for (int i = 0; i < size; i++) {
          String fileName = fileList[i];
          String fullPath = file.getPath() + "/" + fileName;
          File fileOrFolder = new File(fullPath);
          delete(fileOrFolder);
        }
      }
    } else {
      file.delete();
    }
  }

}
