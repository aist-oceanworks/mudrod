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
package esiptestbed.mudrod.metadata.pre;

import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.metadata.structure.MetadataExtractor;
import esiptestbed.mudrod.utils.MatrixUtil;
import esiptestbed.mudrod.utils.RDDUtil;

public class MatrixGenerator extends DiscoveryStepAbstract {

  public MatrixGenerator(Map<String, String> config, ESDriver es,
      SparkDriver spark) {
    super(config, es, spark);
    // TODO Auto-generated constructor stub
  }

  @Override
  public Object execute() {
    // TODO Auto-generated method stub
    System.out
        .println("*****************Metadata matrix starts******************");
    startTime = System.currentTimeMillis();

    String metadata_matrix_file = config.get("metadataMatrix");
    try {
      MetadataExtractor extractor = new MetadataExtractor();
      JavaPairRDD<String, List<String>> metadataTermsRDD = extractor
          .loadMetadata(this.es, this.spark.sc, config.get("indexName"),
              config.get("raw_metadataType"));
      RowMatrix wordDocMatrix = MatrixUtil.createWordDocMatrix(metadataTermsRDD,
          spark.sc);
      List<String> rowKeys = RDDUtil.getAllWordsInDoc(metadataTermsRDD)
          .collect();
      List<String> colKeys = metadataTermsRDD.keys().collect();
      MatrixUtil.exportToCSV(wordDocMatrix, rowKeys, colKeys,
          metadata_matrix_file);

    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    endTime = System.currentTimeMillis();
    System.out
        .println("*****************Metadata matrix ends******************Took "
            + (endTime - startTime) / 1000 + "s");
    return null;
  }

  @Override
  public Object execute(Object o) {
    // TODO Auto-generated method stub
    return null;
  }

}
