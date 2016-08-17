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
package esiptestbed.mudrod.metadata.process;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.semantics.SVDAnalyzer;
import esiptestbed.mudrod.utils.LinkageTriple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ClassName: MetadataAnalyzer 
 * Function: Calculate semantic relationship of vocabularies extracted from
 * metadata. 
 *
 * @author Yun

 */
public class MetadataAnalyzer extends DiscoveryStepAbstract
implements Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory
      .getLogger(MetadataAnalyzer.class);

  /**
   * Creates a new instance of MetadataAnalyzer.
   * 
   * @param props
   *          the Mudrod configuration
   * @param es
   *          the Elasticsearch drive
   * @param spark
   *          the spark drive
   */
  public MetadataAnalyzer(Properties props, ESDriver es,
      SparkDriver spark) {
    super(props, es, spark);
  }

  @Override
  public Object execute(Object o) {
    return null;
  }

  /**
   * Calculate semantic relationship of vocabularies from a csv file which is a
   * term-metadata matrix.
   * 
   * @see esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract#execute()
   */
  @Override
  public Object execute() {
    try {
      LOG.info("*****************Metadata Analyzer starts******************");
      startTime = System.currentTimeMillis();

      SVDAnalyzer analyzer = new SVDAnalyzer(props, es, spark);
      int svdDimension = Integer.parseInt(props.getProperty("metadataSVDDimension"));
      String metadataMatrixFile = props.getProperty("metadataMatrix");
      String svdMatrixFileName = props.getProperty("metadataSVDMatrix_tmp");

      analyzer.getSVDMatrix(metadataMatrixFile, svdDimension,
          svdMatrixFileName);
      List<LinkageTriple> triples = analyzer
          .calTermSimfromMatrix(svdMatrixFileName);

      analyzer.saveToES(triples, props.getProperty("indexName"),
          props.getProperty("metadataLinkageType"));

    } catch (Exception e) {
      e.printStackTrace();
    }

    endTime = System.currentTimeMillis();
    es.refreshIndex();
    LOG.info(
        "*****************Metadata Analyzer ends******************Took {}s",
        (endTime - startTime) / 1000);
    return null;
  }
}
