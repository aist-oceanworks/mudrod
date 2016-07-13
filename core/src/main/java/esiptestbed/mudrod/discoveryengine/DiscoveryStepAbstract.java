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
import java.util.Map;

import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;

public abstract class DiscoveryStepAbstract extends MudrodAbstract {

  public DiscoveryStepAbstract(Map<String, String> config, ESDriver es,
      SparkDriver spark) {
    super(config, es, spark);
    // TODO Auto-generated constructor stub
  }

  public abstract Object execute();

  public abstract Object execute(Object o);

}
