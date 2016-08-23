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
package esiptestbed.mudrod.semantics;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;

import esiptestbed.mudrod.discoveryengine.MudrodAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.utils.LinkageTriple;
import esiptestbed.mudrod.utils.MatrixUtil;
import esiptestbed.mudrod.utils.SimilarityUtil;

/**
 * ClassName: SemanticAnalyzer Function: Semantic analyzer Date: Aug 12, 2016
 * 12:42:55 PM
 *
 * @author Yun
 *
 */
public class SemanticAnalyzer extends MudrodAbstract {

  /**
   * Creates a new instance of SemanticAnalyzer.
   *
   * @param config
   *          the Mudrod configuration
   * @param es
   *          the Elasticsearch drive
   * @param spark
   *          the spark drive
   */
  public SemanticAnalyzer(Map<String, String> config, ESDriver es,
      SparkDriver spark) {
    super(config, es, spark);
    // TODO Auto-generated constructor stub
  }

  /**
   * CalTermSimfromMatrix: Calculate term similarity from matrix.
   *
   * @param CSV_fileName
   *          csv file of matrix, each row is a term, and each column is a
   *          dimension in feature space
   * @return Linkage triple list
   */
  public List<LinkageTriple> CalTermSimfromMatrix(String CSV_fileName) {

    /*
     * JavaPairRDD<String, Vector> importRDD =
     * MatrixUtil.loadVectorFromCSV(spark, CSV_fileName, 1);
     * CoordinateMatrix simMatrix = SimilarityUtil
     * .CalSimilarityFromVector(importRDD.values()); JavaRDD<String>
     * rowKeyRDD = importRDD.keys(); List<LinkageTriple> triples =
     * SimilarityUtil.MatrixtoTriples(rowKeyRDD, simMatrix);
     * 
     * return triples;
     */

    return this.CalTermSimfromMatrix(CSV_fileName, 1);
  }

  public List<LinkageTriple> CalTermSimfromMatrix(String CSV_fileName,
      int skipRow) {

    JavaPairRDD<String, Vector> importRDD = MatrixUtil.loadVectorFromCSV(spark,
        CSV_fileName, skipRow);
    CoordinateMatrix simMatrix = SimilarityUtil
        .CalSimilarityFromVector(importRDD.values());
    JavaRDD<String> rowKeyRDD = importRDD.keys();
    List<LinkageTriple> triples = SimilarityUtil.MatrixtoTriples(rowKeyRDD,
        simMatrix);

    return triples;
  }

  public void SaveToES(List<LinkageTriple> triple_List, String index,
      String type) {
    try {
      LinkageTriple.insertTriples(es, triple_List, index, type);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * SaveToES: Save linkage triples to Elasticsearch.
   *
   * @param triple_List
   *          linkage triple list
   * @param index
   *          index name
   * @param type
   *          linkage triple type name
   */
  public void SaveToES(List<LinkageTriple> triple_List, String index,
      String type, boolean bTriple, boolean bSymmetry) {
    try {
      LinkageTriple.insertTriples(es, triple_List, index, type, bTriple,
          bSymmetry);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
