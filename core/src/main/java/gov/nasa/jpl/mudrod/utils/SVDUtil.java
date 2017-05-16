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
package gov.nasa.jpl.mudrod.utils;

import gov.nasa.jpl.mudrod.discoveryengine.MudrodAbstract;
import gov.nasa.jpl.mudrod.driver.ESDriver;
import gov.nasa.jpl.mudrod.driver.SparkDriver;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * Singular value decomposition
 */
public class SVDUtil extends MudrodAbstract {

  /**
   *
   */
  private static final long serialVersionUID = 1L;
  // wordRDD: terms extracted from all documents
  JavaRDD<String> wordRDD;
  // svdMatrix: svd matrix
  private RowMatrix svdMatrix;
  // simMatrix: similarity matrix
  private CoordinateMatrix simMatrix;

  /**
   * Creates a new instance of SVDUtil.
   *
   * @param config the Mudrod configuration
   * @param es     the Elasticsearch drive
   * @param spark  the spark driver
   */
  public SVDUtil(Properties config, ESDriver es, SparkDriver spark) {
    super(config, es, spark);
  }

  /**
   * Build SVD matrix from docment-terms pairs.
   *
   * @param docwordRDD    JavaPairRDD, key is short name of data set and values are terms in
   *                      the corresponding data set
   * @param svdDimension: Dimension of matrix after singular value decomposition
   * @return row matrix
   */
  public RowMatrix buildSVDMatrix(JavaPairRDD<String, List<String>> docwordRDD, int svdDimension) {

    RowMatrix svdMatrix = null;
    LabeledRowMatrix wordDocMatrix = MatrixUtil.createWordDocMatrix(docwordRDD);
    RowMatrix ifIdfMatrix = MatrixUtil.createTFIDFMatrix(wordDocMatrix.rowMatrix);
    svdMatrix = MatrixUtil.buildSVDMatrix(ifIdfMatrix, svdDimension);
    this.svdMatrix = svdMatrix;
    this.wordRDD = RDDUtil.getAllWordsInDoc(docwordRDD);
    return svdMatrix;
  }

  /**
   * Build svd matrix from CSV file.
   *
   * @param tfidfCSVfile  tf-idf matrix csv file
   * @param svdDimension: Dimension of matrix after singular value decomposition
   * @return row matrix
   */
  public RowMatrix buildSVDMatrix(String tfidfCSVfile, int svdDimension) {
    RowMatrix svdMatrix = null;
    JavaPairRDD<String, Vector> tfidfRDD = MatrixUtil.loadVectorFromCSV(spark, tfidfCSVfile, 2);
    JavaRDD<Vector> vectorRDD = tfidfRDD.values();

    svdMatrix = MatrixUtil.buildSVDMatrix(vectorRDD, svdDimension);
    this.svdMatrix = svdMatrix;

    this.wordRDD = tfidfRDD.keys();

    return svdMatrix;
  }

  /**
   * Calculate similarity
   */
  public void calSimilarity() {
    CoordinateMatrix simMatrix = SimilarityUtil.calculateSimilarityFromMatrix(svdMatrix);
    this.simMatrix = simMatrix;
  }

  /**
   * Insert linkage triples to elasticsearch
   *
   * @param index index name
   * @param type  linkage triple name
   */
  public void insertLinkageToES(String index, String type) {
    List<LinkageTriple> triples = SimilarityUtil.matrixToTriples(wordRDD, simMatrix);
    try {
      LinkageTriple.insertTriples(es, triples, index, type);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
