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
package esiptestbed.mudrod.utils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;

import esiptestbed.mudrod.discoveryengine.MudrodAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;

/**
 * ClassName: SVDUtil Function: Singular value decomposition Date: Aug 15, 2016
 * 1:58:02 PM
 *
 * @author Yun
 *
 */
public class SVDUtil extends MudrodAbstract {

  // wordRDD: terms extracted from all documents
  JavaRDD<String> wordRDD;
  // svdMatrix: svd matrix
  private RowMatrix svdMatrix;
  // simMatrix: similarity matrix
  private CoordinateMatrix simMatrix;

  /**
   * Creates a new instance of SVDUtil.
   *
   * @param config
   *          the Mudrod configuration
   * @param es
   *          the Elasticsearch drive
   * @param spark
   *          the spark driver
   */
  public SVDUtil(Map<String, String> config, ESDriver es, SparkDriver spark) {
    super(config, es, spark);
    // TODO Auto-generated constructor stub
  }

  /**
   * buildSVDMatrix: build svd matrix from docment-terms pairs.
   *
   * @param docwordRDD
   *          JavaPairRDD, key is short name of data set and values are terms in
   *          the corresponding data set
   * @param svdDimension:
   *          Dimension of matrix after singular value decomposition
   * @return row matrix
   */
  public RowMatrix buildSVDMatrix(JavaPairRDD<String, List<String>> docwordRDD,
      int svdDimension) {

    RowMatrix svdMatrix = null;
    LabeledRowMatrix wordDocMatrix = MatrixUtil.createWordDocMatrix(docwordRDD,
        spark.sc);
    RowMatrix TFIDFMatrix = MatrixUtil
        .createTFIDFMatrix(wordDocMatrix.wordDocMatrix, spark.sc);
    svdMatrix = MatrixUtil.buildSVDMatrix(TFIDFMatrix, svdDimension);
    this.svdMatrix = svdMatrix;
    this.wordRDD = RDDUtil.getAllWordsInDoc(docwordRDD);
    return svdMatrix;
  }

  /**
   * buildSVDMatrix: build svd matrix from csv file.
   *
   * @param tfidfCSVfile
   *          tf-idf matrix csv file
   * @param svdDimension:
   *          Dimension of matrix after singular value decomposition
   * @return row matrix
   */
  public RowMatrix buildSVDMatrix(String tfidfCSVfile, int svdDimension) {
    RowMatrix svdMatrix = null;
    JavaPairRDD<String, Vector> tfidfRDD = MatrixUtil.loadVectorFromCSV(spark,
        tfidfCSVfile, 2);
    JavaRDD<Vector> vectorRDD = tfidfRDD.values();

    svdMatrix = MatrixUtil.buildSVDMatrix(vectorRDD, svdDimension);
    this.svdMatrix = svdMatrix;

    this.wordRDD = tfidfRDD.keys();

    return svdMatrix;
  }

  /**
   * CalSimilarity: calculate similarity
   */
  public void CalSimilarity() {
    CoordinateMatrix simMatrix = SimilarityUtil
        .CalSimilarityFromMatrix(svdMatrix);
    this.simMatrix = simMatrix;
  }

  /**
   * insertLinkageToES:insert linkage triples to elasticsearch
   *
   * @param index
   *          index name
   * @param type
   *          linkage triple name
   */
  public void insertLinkageToES(String index, String type) {
    List<LinkageTriple> triples = SimilarityUtil.MatrixtoTriples(wordRDD,
        simMatrix);
    try {
      LinkageTriple.insertTriples(es, triples, index, type);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

}
