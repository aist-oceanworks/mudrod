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

import gov.nasa.jpl.mudrod.driver.ESDriver;
import gov.nasa.jpl.mudrod.driver.SparkDriver;
import gov.nasa.jpl.mudrod.utils.MatrixUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * ClassName: SVDAnalyzer Function: Analyze semantic relationship through SVD
 * method
 */
public class SVDAnalyzer extends SemanticAnalyzer {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  /**
   * Creates a new instance of SVDAnalyzer.
   *
   * @param props the Mudrod configuration
   * @param es    the Elasticsearch drive
   * @param spark the spark drive
   */
  public SVDAnalyzer(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  /**
   * GetSVDMatrix: Create SVD matrix csv file from original csv file.
   *
   * @param csvFileName       each row is a term, and each column is a document.
   * @param svdDimention      Dimension of SVD matrix
   * @param svdMatrixFileName CSV file name of SVD matrix
   */
  public void getSVDMatrix(String csvFileName, int svdDimention, String svdMatrixFileName) {

    JavaPairRDD<String, Vector> importRDD = MatrixUtil.loadVectorFromCSV(spark, csvFileName, 1);
    JavaRDD<Vector> vectorRDD = importRDD.values();
    RowMatrix wordDocMatrix = new RowMatrix(vectorRDD.rdd());
    RowMatrix tfidfMatrix = MatrixUtil.createTFIDFMatrix(wordDocMatrix);
    RowMatrix svdMatrix = MatrixUtil.buildSVDMatrix(tfidfMatrix, svdDimention);

    List<String> rowKeys = importRDD.keys().collect();
    List<String> colKeys = new ArrayList<>();
    for (int i = 0; i < svdDimention; i++) {
      colKeys.add("dimension" + i);
    }
    MatrixUtil.exportToCSV(svdMatrix, rowKeys, colKeys, svdMatrixFileName);
  }
}
