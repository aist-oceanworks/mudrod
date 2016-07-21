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

import java.io.File;
import java.util.ArrayList;
import java.util.Map;

import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.weblog.pre.ClickStreamGenerator;
import esiptestbed.mudrod.weblog.pre.CrawlerDetection;
import esiptestbed.mudrod.weblog.pre.HistoryGenerator;
import esiptestbed.mudrod.weblog.pre.ImportLogFile;
import esiptestbed.mudrod.weblog.pre.RemoveRawLog;
import esiptestbed.mudrod.weblog.pre.SessionGenerator;
import esiptestbed.mudrod.weblog.pre.SessionStatistic;
import esiptestbed.mudrod.weblog.process.ClickStreamAnalyzer;
import esiptestbed.mudrod.weblog.process.UserHistoryAnalyzer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WeblogDiscoveryEngine extends DiscoveryEngineAbstract {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(WeblogDiscoveryEngine.class);
  public WeblogDiscoveryEngine(Map<String, String> config, ESDriver es, SparkDriver spark){
    super(config, es, spark);
  }

  public String timeSuffix = null;

  @Override
  public void preprocess() {
    LOG.info("*****************Web log preprocessing starts******************");

    File directory = new File(config.get("logDir"));

    ArrayList<String> inputList = new ArrayList<>();
    // get all the files from a directory
    File[] fList = directory.listFiles();
    for (File file : fList) {
      if (file.isFile()) {

      } else if (file.isDirectory() && file.getName().matches(".*\\d+.*") && file.getName().contains(config.get("httpPrefix"))) {
        inputList.add(file.getName().replace(config.get("httpPrefix"), ""));
      }
    }

    for(int i =0; i < inputList.size(); i++){
      timeSuffix = inputList.get(i);
      config.put("TimeSuffix", timeSuffix);
      startTime=System.currentTimeMillis();
      LOG.info("*****************Web log preprocessing starts****************** {}", inputList.get(i));

      DiscoveryStepAbstract im = new ImportLogFile(this.config, this.es, this.spark);
      im.execute();

      DiscoveryStepAbstract cd = new CrawlerDetection(this.config, this.es, this.spark);
      cd.execute();

      DiscoveryStepAbstract sg = new SessionGenerator(this.config, this.es, this.spark);
      sg.execute();

      DiscoveryStepAbstract ss = new SessionStatistic(this.config, this.es, this.spark);
      ss.execute();

      DiscoveryStepAbstract rr = new RemoveRawLog(this.config, this.es, this.spark);
      rr.execute();

      endTime=System.currentTimeMillis();

      LOG.info("*****************Web log preprocessing ends******************Took {}s {}", 
          (endTime-startTime)/1000, inputList.get(i));
    }

    DiscoveryStepAbstract hg = new HistoryGenerator(this.config, this.es, this.spark);
    hg.execute();

    DiscoveryStepAbstract cg = new ClickStreamGenerator(this.config, this.es, this.spark);
    cg.execute();

    LOG.info("*****************Web log preprocessing (user history and clickstream finished) ends******************");

  }


  @Override
  public void process() {
    LOG.info("*****************Web log processing starts******************");
    startTime=System.currentTimeMillis();

    DiscoveryStepAbstract svd = new ClickStreamAnalyzer(this.config, this.es, this.spark);
    svd.execute();

    DiscoveryStepAbstract ua = new UserHistoryAnalyzer(this.config, this.es, this.spark);
    ua.execute();

    endTime=System.currentTimeMillis();
    LOG.info("*****************Web log processing ends******************Took {}s", (endTime-startTime)/1000);
  }

  @Override
  public void output() {
  }

}
