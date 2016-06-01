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

import java.util.ArrayList;
import java.util.Map;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;

/**
 * Say sth
 * 
 * 
 * 
 * 
 */

public class Example extends DiscoveryStepAbstract {
  public Example(Map<String, String> config, ESDriver es) {
    super(config, es);
    // TODO Auto-generated constructor stub
  }

  @Override
  public ArrayList<String> execute() {
    // TODO Auto-generated method stub
    System.out.println("*****************Step 1: Example******************");
    startTime=System.currentTimeMillis();
    es.createBulkProcesser();
    /* Do something */

    es.destroyBulkProcessor();
    endTime=System.currentTimeMillis();
    System.out.println("*****************Example ends******************Took " + (endTime-startTime)/1000+"s");
    return null;
  }



  @Override
  public Object execute(Object o) {
    // TODO Auto-generated method stub
    return null;
  }

}
