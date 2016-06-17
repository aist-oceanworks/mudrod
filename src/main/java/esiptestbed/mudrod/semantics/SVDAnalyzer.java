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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;

import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.utils.MatrixUtil;

public class SVDAnalyzer extends SemanticAnalyzer {

  public SVDAnalyzer(Map<String, String> config, ESDriver es,
      SparkDriver spark) {
    super(config, es, spark);
    // TODO Auto-generated constructor stub
  }

  public void GetSVDMatrix(String CSV_fileName, int svdDimention,
      String svd_matrix_fileName) {

    try {
      JavaPairRDD<String, Vector> importRDD = MatrixUtil
          .loadVectorFromCSV(spark, CSV_fileName, 1);
      JavaRDD<Vector> vectorRDD = importRDD.values();
      RowMatrix wordDocMatrix = new RowMatrix(vectorRDD.rdd());
      RowMatrix TFIDFMatrix = MatrixUtil.createTFIDFMatrix(wordDocMatrix,
          spark.sc);
      RowMatrix svdMatrix = MatrixUtil.buildSVDMatrix(TFIDFMatrix,
          svdDimention);

      List<String> rowKeys = importRDD.keys().collect();
      List<String> colKeys = new ArrayList<String>();
      for (int i = 0; i < svdDimention; i++) {
        colKeys.add("dimension" + i);
      }
      MatrixUtil.exportToCSV(svdMatrix, rowKeys, colKeys, svd_matrix_fileName);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
