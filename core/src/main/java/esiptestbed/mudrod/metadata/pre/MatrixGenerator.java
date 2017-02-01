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
import java.util.Properties;

import org.apache.spark.api.java.JavaPairRDD;
import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.metadata.structure.MetadataExtractor;
import esiptestbed.mudrod.utils.LabeledRowMatrix;
import esiptestbed.mudrod.utils.MatrixUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ClassName: MatrixGenerator 
 * Function: Generate term-metadata matrix from original metadata. Each row in
 * the matrix is corresponding to a term, and each column is a metadata. 
 */
public class MatrixGenerator extends DiscoveryStepAbstract {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(MatrixGenerator.class);

  /**
   * Creates a new instance of MatrixGenerator.
   * 
   * @param props
   *            the Mudrod configuration
   * @param es
   *            the Elasticsearch drive
   * @param spark
   *            the spark drive
   */
  public MatrixGenerator(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  /**
   * Generate a csv which is a term-metadata matrix genetrated from original
   * metadata.
   * 
   * @see esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract#execute()
   */
  @Override
  public Object execute() {
    LOG.info("*****************Metadata matrix starts******************");
    startTime = System.currentTimeMillis();

    String metadataMatrixFile = props.getProperty("metadataMatrix");
    try {
      MetadataExtractor extractor = new MetadataExtractor();
      JavaPairRDD<String, List<String>> metadataTermsRDD = extractor
          .loadMetadata(this.es, this.spark.sc, props.getProperty("indexName"),
              props.getProperty("raw_metadataType"));
      LabeledRowMatrix wordDocMatrix = MatrixUtil
          .createWordDocMatrix(metadataTermsRDD, spark.sc);
      MatrixUtil.exportToCSV(wordDocMatrix.rowMatrix, wordDocMatrix.rowkeys,
          wordDocMatrix.colkeys, metadataMatrixFile);

    } catch (Exception e) {
      e.printStackTrace();
    }

    endTime = System.currentTimeMillis();
    LOG.info("*****************Metadata matrix ends******************Took {}s", (endTime - startTime) / 1000);
    return null;
  }

  @Override
  public Object execute(Object o) {
    return null;
  }

}
