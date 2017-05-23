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
package gov.nasa.jpl.mudrod.weblog.process;

import gov.nasa.jpl.mudrod.discoveryengine.DiscoveryStepAbstract;
import gov.nasa.jpl.mudrod.driver.ESDriver;
import gov.nasa.jpl.mudrod.driver.SparkDriver;
import gov.nasa.jpl.mudrod.semantics.SVDAnalyzer;
import gov.nasa.jpl.mudrod.ssearch.ClickstreamImporter;
import gov.nasa.jpl.mudrod.utils.LinkageTriple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Properties;

/**
 * Supports ability to calculate term similarity based on click stream
 */
public class ClickStreamAnalyzer extends DiscoveryStepAbstract {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(ClickStreamAnalyzer.class);

  public ClickStreamAnalyzer(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  /**
   * Method of executing click stream analyzer
   */
  @Override
  public Object execute() {
    LOG.info("Starting ClickStreamAnalyzer...");
    startTime = System.currentTimeMillis();
    try {
      String clickstream_matrixFile = props.getProperty("clickstreamMatrix");
      File f = new File(clickstream_matrixFile);
      if (f.exists()) {
        SVDAnalyzer svd = new SVDAnalyzer(props, es, spark);
        svd.getSVDMatrix(props.getProperty("clickstreamMatrix"), Integer.parseInt(props.getProperty("clickstreamSVDDimension")), props.getProperty("clickstreamSVDMatrix_tmp"));
        List<LinkageTriple> tripleList = svd.calTermSimfromMatrix(props.getProperty("clickstreamSVDMatrix_tmp"));
        svd.saveToES(tripleList, props.getProperty("indexName"), props.getProperty("clickStreamLinkageType"));
      
        // Store click stream in ES for the ranking use
        ClickstreamImporter cs = new ClickstreamImporter(props, es, spark);
        cs.importfromCSVtoES();
      }
    } catch (Exception e) {
      LOG.error("Encountered an error during execution of ClickStreamAnalyzer.", e);
    }

    endTime = System.currentTimeMillis();
    es.refreshIndex();
    LOG.info("ClickStreamAnalyzer complete. Time elapsed: {}s", (endTime - startTime) / 1000);
    return null;
  }

  @Override
  public Object execute(Object o) {
    return null;
  }
}
