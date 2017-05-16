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
package gov.nasa.jpl.mudrod.weblog.pre;

import gov.nasa.jpl.mudrod.discoveryengine.DiscoveryStepAbstract;
import gov.nasa.jpl.mudrod.driver.ESDriver;
import gov.nasa.jpl.mudrod.driver.SparkDriver;
import gov.nasa.jpl.mudrod.utils.LabeledRowMatrix;
import gov.nasa.jpl.mudrod.utils.MatrixUtil;
import gov.nasa.jpl.mudrod.weblog.structure.ClickStream;
import gov.nasa.jpl.mudrod.weblog.structure.SessionExtractor;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

/**
 * Supports ability to extract click stream data based on session processing results
 */
public class ClickStreamGenerator extends DiscoveryStepAbstract {

  /**
   *
   */
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(ClickStreamGenerator.class);

  public ClickStreamGenerator(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  @Override
  public Object execute() {
    LOG.info("Starting ClickStreamGenerator...");
    startTime = System.currentTimeMillis();

    String clickstremMatrixFile = props.getProperty("clickstreamMatrix");
    try {
      SessionExtractor extractor = new SessionExtractor();
      JavaRDD<ClickStream> clickstreamRDD = extractor.extractClickStreamFromES(this.props, this.es, this.spark);
      int weight = Integer.parseInt(props.getProperty("downloadWeight"));
      JavaPairRDD<String, List<String>> metaddataQueryRDD = extractor.bulidDataQueryRDD(clickstreamRDD, weight);
      LabeledRowMatrix wordDocMatrix = MatrixUtil.createWordDocMatrix(metaddataQueryRDD);

      MatrixUtil.exportToCSV(wordDocMatrix.rowMatrix, wordDocMatrix.rowkeys, wordDocMatrix.colkeys, clickstremMatrixFile);
    } catch (Exception e) {
      LOG.error("Encountered error within ClickStreamGenerator: {}", e);
    }

    endTime = System.currentTimeMillis();
    LOG.info("ClickStreamGenerator complete. Time elapsed {} seconds.", (endTime - startTime) / 1000);
    return null;
  }

  @Override
  public Object execute(Object o) {
    return null;
  }

}
