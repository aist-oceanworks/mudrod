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

import java.util.Properties;

/*
 * Generic class of discovery engine step
 */
public abstract class DiscoveryStepAbstract extends MudrodAbstract {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  public DiscoveryStepAbstract(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  /**
   * Abstract class of step execution without parameter
   *
   * @return An instance of Object
   */
  public abstract Object execute();

  /**
   * Abstract class of step execution with parameter
   *
   * @param o an instance of object
   * @return An instance of object
   */
  public abstract Object execute(Object o);

}