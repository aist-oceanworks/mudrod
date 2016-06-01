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

import java.util.Map;


import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.weblog.pre.CrawlerDetection;
import esiptestbed.mudrod.weblog.pre.ImportLogFile;
import esiptestbed.mudrod.weblog.pre.RemoveRawLog;
import esiptestbed.mudrod.weblog.pre.SessionGenerator;
import esiptestbed.mudrod.weblog.pre.SessionStatistic;

public class WeblogDiscoveryEngine extends DiscoveryEngineAbstract {		
  public WeblogDiscoveryEngine(Map<String, String> config, ESDriver es){
    super(config, es);
  }

  @Override
  public void preprocess() {
    // TODO Auto-generated method stub	
    System.out.println("*****************Preprocessing starts******************");
    startTime=System.currentTimeMillis();

    DiscoveryStepAbstract im = new ImportLogFile(this.config, this.es);
    im.execute();

    DiscoveryStepAbstract cd = new CrawlerDetection(this.config, this.es);
    cd.execute();

    DiscoveryStepAbstract sg = new SessionGenerator(this.config, this.es);
    sg.execute();

    DiscoveryStepAbstract ss = new SessionStatistic(this.config, this.es);
    ss.execute();

    DiscoveryStepAbstract rr = new RemoveRawLog(this.config, this.es);
    rr.execute();

    endTime=System.currentTimeMillis();
    System.out.println("*****************Preprocessing ends******************Took " + (endTime-startTime)/1000+"s");

  }


  @Override
  public void process() {
    // TODO Auto-generated method stub

  }

  @Override
  public void output() {
    // TODO Auto-generated method stub

  }

}
