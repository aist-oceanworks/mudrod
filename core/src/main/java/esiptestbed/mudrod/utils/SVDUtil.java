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

public class SVDUtil extends MudrodAbstract {

  JavaRDD<String> wordRDD;
  private RowMatrix svdMatrix;
  private CoordinateMatrix simMatrix;

  public SVDUtil(Map<String, String> config, ESDriver es, SparkDriver spark) {
    super(config, es, spark);
    // TODO Auto-generated constructor stub
  }

  public RowMatrix buildSVDMatrix(JavaPairRDD<String, List<String>> docwordRDD,
      int svdDimension) {

    RowMatrix svdMatrix = null;
    try {
      RowMatrix wordDocMatrix = MatrixUtil.createWordDocMatrix(docwordRDD,
          spark.sc);
      RowMatrix TFIDFMatrix = MatrixUtil.createTFIDFMatrix(wordDocMatrix,
          spark.sc);
      svdMatrix = MatrixUtil.buildSVDMatrix(TFIDFMatrix, svdDimension);
      this.svdMatrix = svdMatrix;
      this.wordRDD = RDDUtil.getAllWordsInDoc(docwordRDD);
      // exportSVDMatrixToCSV example
      // MatrixUtil.exportSVDMatrixToCSV(wordDocMatrix, wordRDD.collect(),
      // docwordRDD.keys().collect(), "D:/wordDocMatrix.csv");
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return svdMatrix;
  }

  public RowMatrix buildSVDMatrix(String tfidfCSVfile, int svdDimension) {
    RowMatrix svdMatrix = null;
    JavaPairRDD<String, Vector> tfidfRDD = MatrixUtil.loadVectorFromCSV(spark,
        tfidfCSVfile, 2);
    JavaRDD<Vector> vectorRDD = tfidfRDD.values();

    try {
      svdMatrix = MatrixUtil.buildSVDMatrix(vectorRDD, svdDimension);
      this.svdMatrix = svdMatrix;
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    this.wordRDD = tfidfRDD.keys();

    return svdMatrix;
  }

  /*
   * public RowMatrix buildSVDMatrix(Map<String, List<String>> docwords){
   * 
   * return null; }
   */

  public void CalSimilarity() {
    CoordinateMatrix simMatrix = SimilarityUtil
        .CalSimilarityFromMatrix(svdMatrix);
    this.simMatrix = simMatrix;
  }

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
