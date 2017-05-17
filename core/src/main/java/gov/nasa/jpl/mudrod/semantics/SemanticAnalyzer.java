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
package gov.nasa.jpl.mudrod.semantics;

import gov.nasa.jpl.mudrod.discoveryengine.MudrodAbstract;
import gov.nasa.jpl.mudrod.driver.ESDriver;
import gov.nasa.jpl.mudrod.driver.SparkDriver;
import gov.nasa.jpl.mudrod.utils.LinkageTriple;
import gov.nasa.jpl.mudrod.utils.MatrixUtil;
import gov.nasa.jpl.mudrod.utils.SimilarityUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * ClassName: SemanticAnalyzer Function: Semantic analyzer
 */
public class SemanticAnalyzer extends MudrodAbstract {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  /**
   * Creates a new instance of SemanticAnalyzer.
   *
   * @param props
   *          the Mudrod configuration
   * @param es
   *          the Elasticsearch drive
   * @param spark
   *          the spark drive
   */
  public SemanticAnalyzer(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  /**
   * Calculate term similarity from CSV matrix.
   *
   * @param csvFileName
   *          csv file of matrix, each row is a term, and each column is a
   *          dimension in feature space
   * @return Linkage triple list
   */
  public List<LinkageTriple> calTermSimfromMatrix(String csvFileName) {
    File f = new File(csvFileName);
    if (!f.exists()) {
      return null;
    }
    return this.calTermSimfromMatrix(csvFileName, 1);
  }

  /**
   * Calculate term similarity from CSV matrix.
   *
   * @param csvFileName csv file of matrix, each row is a term, and each column is a
   *                    dimension in feature space
   * @param skipRow number of rows to skip in input CSV file e.g. header
   * @return Linkage triple list
   */
  public List<LinkageTriple> calTermSimfromMatrix(String csvFileName, int skipRow) {

    JavaPairRDD<String, Vector> importRDD = MatrixUtil.loadVectorFromCSV(spark, csvFileName, skipRow);
    if (importRDD == null || importRDD.values().first().size() == 0) {
      return null;
    }

    CoordinateMatrix simMatrix = SimilarityUtil.calculateSimilarityFromVector(importRDD.values());
    JavaRDD<String> rowKeyRDD = importRDD.keys();
    return SimilarityUtil.matrixToTriples(rowKeyRDD, simMatrix);
  }

  /**
   * Calculate term similarity from CSV matrix.
   *
   * @param csvFileName csv file of matrix, each row is a term, and each column is a
   *                    dimension in feature space
   * @param simType the type of similary calculation to execute e.g.
   * <ul>
   * <li>{@link gov.nasa.jpl.mudrod.utils.SimilarityUtil#SIM_COSINE} - 3,</li>
   * <li>{@link gov.nasa.jpl.mudrod.utils.SimilarityUtil#SIM_HELLINGER} - 2,</li>
   * <li>{@link gov.nasa.jpl.mudrod.utils.SimilarityUtil#SIM_PEARSON} - 1</li>
   * </ul>
   * @param skipRow number of rows to skip in input CSV file e.g. header
   * @return Linkage triple list
   */
  public List<LinkageTriple> calTermSimfromMatrix(String csvFileName, int simType, int skipRow) {

    JavaPairRDD<String, Vector> importRDD = MatrixUtil.loadVectorFromCSV(spark, csvFileName, skipRow);
    if (importRDD.values().first().size() == 0) {
      return null;
    }

    JavaRDD<LinkageTriple> triples = SimilarityUtil.calculateSimilarityFromVector(importRDD, simType);

    return triples.collect();
  }

  public void saveToES(List<LinkageTriple> tripleList, String index, String type) {
    try {
      LinkageTriple.insertTriples(es, tripleList, index, type);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Method of saving linkage triples to Elasticsearch.
   *
   * @param tripleList
   *          linkage triple list
   * @param index
   *          index name
   * @param type
   *          type name
   * @param bTriple
   *          bTriple
   * @param bSymmetry
   *          bSymmetry
   */
  public void saveToES(List<LinkageTriple> tripleList, String index, String type, boolean bTriple, boolean bSymmetry) {
    try {
      LinkageTriple.insertTriples(es, tripleList, index, type, bTriple, bSymmetry);
    } catch (IOException e) {
      e.printStackTrace();

    }
  }
}
