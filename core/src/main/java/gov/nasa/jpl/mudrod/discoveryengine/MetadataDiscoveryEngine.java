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
package gov.nasa.jpl.mudrod.discoveryengine;

import gov.nasa.jpl.mudrod.driver.ESDriver;
import gov.nasa.jpl.mudrod.driver.SparkDriver;
import gov.nasa.jpl.mudrod.metadata.pre.ApiHarvester;
import gov.nasa.jpl.mudrod.metadata.pre.MatrixGenerator;
import gov.nasa.jpl.mudrod.metadata.process.MetadataAnalyzer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Properties;

/**
 * Supports to preprocess and process metadata
 */
public class MetadataDiscoveryEngine extends DiscoveryEngineAbstract implements Serializable {

  /**
   *
   */
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(MetadataDiscoveryEngine.class);

  public MetadataDiscoveryEngine(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  /**
   * Method of preprocessing metadata
   */
  public void preprocess() {
    LOG.info("Starting metadata preprocessing...");
    startTime = System.currentTimeMillis();

    DiscoveryStepAbstract harvester = new ApiHarvester(this.props, this.es, this.spark);
    harvester.execute();

    endTime = System.currentTimeMillis();
    LOG.info("Finished metadata preprocessing. Time elapsed: {}s", (endTime - startTime) / 1000);
  }

  /**
   * Method of processing metadata
   */
  public void process() {
    LOG.info("Starting metadata processing...");
    startTime = System.currentTimeMillis();

    DiscoveryStepAbstract matrix = new MatrixGenerator(this.props, this.es, this.spark);
    matrix.execute();

    DiscoveryStepAbstract svd = new MetadataAnalyzer(this.props, this.es, this.spark);
    svd.execute();

    endTime = System.currentTimeMillis();
    LOG.info("Finished metadata processing. Time elapsed: {}s", (endTime - startTime) / 1000);
  }

  public void output() {
  }
}
