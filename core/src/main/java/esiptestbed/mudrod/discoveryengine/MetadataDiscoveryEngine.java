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
package esiptestbed.mudrod.discoveryengine;

import java.io.Serializable;
import java.util.Properties;

import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.metadata.pre.ApiHarvester;
import esiptestbed.mudrod.metadata.pre.MatrixGenerator;
import esiptestbed.mudrod.metadata.process.MetadataAnalyzer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetadataDiscoveryEngine extends DiscoveryEngineAbstract implements Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(MetadataDiscoveryEngine.class);

  public MetadataDiscoveryEngine(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  public void preprocess() {
    LOG.info("*****************Metadata preprocessing starts******************");
    startTime = System.currentTimeMillis();

    DiscoveryStepAbstract harvester = new ApiHarvester(this.props, this.es, this.spark);
    harvester.execute();

    endTime = System.currentTimeMillis();
    LOG.info("*****************Metadata preprocessing ends******************Took {}s",
        (endTime - startTime) / 1000);
  }

  public void process() {
    LOG.info("*****************Metadata processing starts******************");
    startTime = System.currentTimeMillis();

    DiscoveryStepAbstract matrix = new MatrixGenerator(this.props, this.es, this.spark);
    matrix.execute();

    DiscoveryStepAbstract svd = new MetadataAnalyzer(this.props, this.es, this.spark);
    svd.execute();

    endTime = System.currentTimeMillis();
    LOG.info("*****************Metadata processing ends******************Took {}s",
        (endTime - startTime) / 1000);
  }

  public void output() {
  }
}
