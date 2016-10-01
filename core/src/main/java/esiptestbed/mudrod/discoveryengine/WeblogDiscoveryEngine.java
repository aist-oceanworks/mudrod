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
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.main.MudrodConstants;
import esiptestbed.mudrod.weblog.pre.ClickStreamGenerator;
import esiptestbed.mudrod.weblog.pre.CrawlerDetection;
import esiptestbed.mudrod.weblog.pre.HistoryGenerator;
import esiptestbed.mudrod.weblog.pre.ImportLogFile;
import esiptestbed.mudrod.weblog.pre.SessionGenerator;
import esiptestbed.mudrod.weblog.pre.SessionStatistic;
import esiptestbed.mudrod.weblog.process.ClickStreamAnalyzer;
import esiptestbed.mudrod.weblog.process.UserHistoryAnalyzer;

/**
 * Supports to preprocess and process web log
 */
public class WeblogDiscoveryEngine extends DiscoveryEngineAbstract {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory
      .getLogger(WeblogDiscoveryEngine.class);

  public WeblogDiscoveryEngine(Properties props, ESDriver es,
      SparkDriver spark) {
    super(props, es, spark);
    LOG.info("Started Mudrod Weblog Discovery Engine.");
  }

  public String timeSuffix = null;

  /**
   * Get log file list from a directory
   * 
   * @param path
   *          folder directory
   * @return a list of log files
   */
  public ArrayList<String> getFileList(String path) {
    File directory = new File(path);
    ArrayList<String> inputList = new ArrayList<>();
    File[] fList = directory.listFiles();
    for (File file : fList) {
      if (file.isFile()) {

      } else if (file.isDirectory() && file.getName().matches(".*\\d+.*")
          && file.getName().contains(props.getProperty("httpPrefix"))) {
        inputList
            .add(file.getName().replace(props.getProperty("httpPrefix"), ""));
      }
    }
    return inputList;
  }

  /**
   * Method of preprocessing web logs, generating vocab similarity based on web
   * logs
   */
  @Override
  public void preprocess() {
    LOG.info("Starting Web log preprocessing.");

    File directory = new File(props.getProperty("logDir"));

    ArrayList<String> inputList = new ArrayList<>();
    File[] fList = directory.listFiles();
    for (File file : fList) {
      if (file.isFile()) {

      } else if (file.isDirectory() && file.getName().matches(".*\\d+.*")
          && file.getName().contains(props.getProperty("httpPrefix"))) {
        inputList
            .add(file.getName().replace(props.getProperty("httpPrefix"), ""));
      }
    }

    for (int i = 0; i < inputList.size(); i++) {
      timeSuffix = inputList.get(i);
      props.put("TimeSuffix", timeSuffix);
      startTime = System.currentTimeMillis();
      LOG.info("Processing logs dated {}", inputList.get(i));

      /*DiscoveryStepAbstract im = new ImportLogFile(this.props, this.es, this.spark);
      im.execute();*/

      DiscoveryStepAbstract cd = new CrawlerDetection(this.props, this.es,
          this.spark);
      cd.execute();

      DiscoveryStepAbstract sg = new SessionGenerator(this.props, this.es,
          this.spark);
      sg.execute();

      DiscoveryStepAbstract ss = new SessionStatistic(this.props, this.es,
          this.spark);
      ss.execute();

      /*DiscoveryStepAbstract rr = new RemoveRawLog(this.props, this.es,
          this.spark);
      rr.execute();*/

      endTime = System.currentTimeMillis();

      LOG.info(
          "Web log preprocessing for duration {} complete. Time elapsed {} seconds.",
          inputList.get(i), (endTime - startTime) / 1000);
    }

    DiscoveryStepAbstract hg = new HistoryGenerator(this.props, this.es,
        this.spark);
    hg.execute();

    DiscoveryStepAbstract cg = new ClickStreamGenerator(this.props, this.es,
        this.spark);
    cg.execute();

    LOG.info(
        "Web log preprocessing (user history and clickstream finished) complete.");

  }

  /**
   * Method of web log ingest
   */
  public void logIngest() {
    LOG.info("Starting Web log ingest.");

    File directory = new File(props.getProperty("logDir"));

    ArrayList<String> inputList = new ArrayList<>();
    File[] fList = directory.listFiles();
    for (File file : fList) {
      if (file.isFile()) {
        // don't do anything with files, we are only interested in logs which
        // are kept within directories.
      } else if (file.isDirectory() && file.getName().matches(".*\\d+.*")
          && file.getName()
              .contains(props.getProperty(MudrodConstants.HTTP_PREFIX))) {
        inputList.add(file.getName()
            .replace(props.getProperty(MudrodConstants.HTTP_PREFIX), ""));
      }
    }

    for (int i = 0; i < inputList.size(); i++) {
      timeSuffix = inputList.get(i);
      props.put("TimeSuffix", timeSuffix);
      DiscoveryStepAbstract im = new ImportLogFile(this.props, this.es,
          this.spark);
      im.execute();
    }

    LOG.info("Web log ingest complete.");

  }

  /**
   * Method of reconstructing user sessions from raw web logs
   */
  public void sessionRestruct() {
    LOG.info("Starting Session reconstruction.");
    ArrayList<String> inputList = getFileList(props.getProperty("logDir"));
    for (int i = 0; i < inputList.size(); i++) {
      timeSuffix = inputList.get(i); // change timeSuffix dynamically
      props.put("TimeSuffix", timeSuffix);
      DiscoveryStepAbstract cd = new CrawlerDetection(this.props, this.es,
          this.spark);
      cd.execute();

      DiscoveryStepAbstract sg = new SessionGenerator(this.props, this.es,
          this.spark);
      sg.execute();

      DiscoveryStepAbstract ss = new SessionStatistic(this.props, this.es,
          this.spark);
      ss.execute();

      /* DiscoveryStepAbstract rr = new RemoveRawLog(this.props, this.es,
          this.spark);
      rr.execute();*/

      endTime = System.currentTimeMillis();
    }

    /*DiscoveryStepAbstract hg = new HistoryGenerator(this.props, this.es,
        this.spark);
    hg.execute();
    
    DiscoveryStepAbstract cg = new ClickStreamGenerator(this.props, this.es,
        this.spark);
    cg.execute();*/
    LOG.info("Session reconstruction complete.");
  }

  @Override
  public void process() {
    LOG.info("Starting Web log processing.");
    startTime = System.currentTimeMillis();

    DiscoveryStepAbstract svd = new ClickStreamAnalyzer(this.props, this.es,
        this.spark);
    svd.execute();

    DiscoveryStepAbstract ua = new UserHistoryAnalyzer(this.props, this.es,
        this.spark);
    ua.execute();

    endTime = System.currentTimeMillis();
    LOG.info("Web log processing complete. Time elaspsed {} seconds.",
        (endTime - startTime) / 1000);
  }

  @Override
  public void output() {
  }

}
