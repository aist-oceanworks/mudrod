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

import java.io.Serializable;
import java.util.Properties;

public abstract class DiscoveryEngineAbstract extends MudrodAbstract implements Serializable {
  /**
   *
   */
  private static final long serialVersionUID = 1L;

  public DiscoveryEngineAbstract(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  /**
   * Abstract method of preprocess
   */
  public abstract void preprocess();

  /**
   * Abstract method of process
   */
  public abstract void process();

  /**
   * Abstract method of output
   */
  public abstract void output();
}