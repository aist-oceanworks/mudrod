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
package gov.nasa.jpl.mudrod.metadata.pre;

import gov.nasa.jpl.mudrod.discoveryengine.DiscoveryStepAbstract;
import gov.nasa.jpl.mudrod.driver.ESDriver;
import gov.nasa.jpl.mudrod.driver.SparkDriver;
import gov.nasa.jpl.mudrod.main.MudrodConstants;
import gov.nasa.jpl.mudrod.metadata.structure.MetadataExtractor;
import gov.nasa.jpl.mudrod.utils.LabeledRowMatrix;
import gov.nasa.jpl.mudrod.utils.MatrixUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

/**
 * Generate term-metadata matrix from original metadata. Each row in
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
   * @param props the Mudrod configuration
   * @param es    the Elasticsearch drive
   * @param spark the spark drive
   */
  public MatrixGenerator(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  /**
   * Generate a csv which is a term-metadata matrix genetrated from original
   * metadata.
   *
   * @see DiscoveryStepAbstract#execute()
   */
  @Override
  public Object execute() {
    LOG.info("Metadata matrix started");
    startTime = System.currentTimeMillis();

    String metadataMatrixFile = props.getProperty("metadataMatrix");
    try {
      MetadataExtractor extractor = new MetadataExtractor();
      JavaPairRDD<String, List<String>> metadataTermsRDD = extractor.loadMetadata(this.es, this.spark.sc, props.getProperty(MudrodConstants.ES_INDEX_NAME), props.getProperty(MudrodConstants.RAW_METADATA_TYPE));
      LabeledRowMatrix wordDocMatrix = MatrixUtil.createWordDocMatrix(metadataTermsRDD);
      MatrixUtil.exportToCSV(wordDocMatrix.rowMatrix, wordDocMatrix.rowkeys, wordDocMatrix.colkeys, metadataMatrixFile);

    } catch (Exception e) {
      LOG.error("Error during Metadata matrix generaion: {}", e);
    }

    endTime = System.currentTimeMillis();
    LOG.info("Metadata matrix finished time elapsed: {}s", (endTime - startTime) / 1000);
    return null;
  }

  @Override
  public Object execute(Object o) {
    return null;
  }

}
