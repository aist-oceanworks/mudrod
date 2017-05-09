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
import gov.nasa.jpl.mudrod.main.MudrodConstants;
import gov.nasa.jpl.mudrod.weblog.pre.*;
import gov.nasa.jpl.mudrod.weblog.process.ClickStreamAnalyzer;
import gov.nasa.jpl.mudrod.weblog.process.UserHistoryAnalyzer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Supports to preprocess and process web log
 */
public class WeblogDiscoveryEngine extends DiscoveryEngineAbstract {

  /**
   *
   */
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(WeblogDiscoveryEngine.class);
  public String timeSuffix = null;

  public WeblogDiscoveryEngine(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
    LOG.info("Started Mudrod Weblog Discovery Engine.");
  }

  /**
   * Get log file list from a directory
   *
   * @param logDir path to directory containing logs either local or in HDFS.
   * @return a list of log files
   */
  public List<String> getFileList(String logDir) {

    ArrayList<String> inputList = new ArrayList<>();
    if (!logDir.startsWith("hdfs://")) {
      File directory = new File(logDir);
      File[] fList = directory.listFiles();
      for (File file : fList) {
        if (file.isFile() && file.getName().matches(".*\\d+.*") && file.getName().contains(props.getProperty(MudrodConstants.HTTP_PREFIX))) {
          inputList.add(file.getName().replace(props.getProperty(MudrodConstants.HTTP_PREFIX), ""));
        }
      }
    } else {
      Configuration conf = new Configuration();
      try (FileSystem fs = FileSystem.get(new URI(logDir), conf)) {
        FileStatus[] fileStatus;
        fileStatus = fs.listStatus(new Path(logDir));
        for (FileStatus status : fileStatus) {
          String path1 = status.getPath().toString();
          if (path1.matches(".*\\d+.*") && path1.contains(props.getProperty(MudrodConstants.HTTP_PREFIX))) {

            String time = path1.substring(path1.lastIndexOf('.') + 1);
            inputList.add(time);
          }
        }
      } catch (IllegalArgumentException | IOException | URISyntaxException e) {
        LOG.error("An error occured whilst obtaining the log file list.", e);
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

    ArrayList<String> inputList = (ArrayList<String>) getFileList(props.getProperty(MudrodConstants.DATA_DIR));

    for (int i = 0; i < inputList.size(); i++) {
      timeSuffix = inputList.get(i);
      props.put(MudrodConstants.TIME_SUFFIX, timeSuffix);
      startTime = System.currentTimeMillis();
      LOG.info("Processing logs dated {}", inputList.get(i));

      DiscoveryStepAbstract im = new ImportLogFile(this.props, this.es, this.spark);
      im.execute();

      DiscoveryStepAbstract cd = new CrawlerDetection(this.props, this.es, this.spark);
      cd.execute();

      DiscoveryStepAbstract sg = new SessionGenerator(this.props, this.es, this.spark);
      sg.execute();

      DiscoveryStepAbstract ss = new SessionStatistic(this.props, this.es, this.spark);
      ss.execute();

      DiscoveryStepAbstract rr = new RemoveRawLog(this.props, this.es, this.spark);
      rr.execute();

      endTime = System.currentTimeMillis();

      LOG.info("Web log preprocessing for logs dated {} complete. Time elapsed {} seconds.", inputList.get(i), (endTime - startTime) / 1000);
    }

    DiscoveryStepAbstract hg = new HistoryGenerator(this.props, this.es, this.spark);
    hg.execute();

    DiscoveryStepAbstract cg = new ClickStreamGenerator(this.props, this.es, this.spark);
    cg.execute();

    LOG.info("Web log preprocessing (user history and clickstream) complete.");
  }

  /**
   * Method of web log ingest
   */
  public void logIngest() {
    LOG.info("Starting Web log ingest.");
    ArrayList<String> inputList = (ArrayList<String>) getFileList(props.getProperty(MudrodConstants.DATA_DIR));
    for (int i = 0; i < inputList.size(); i++) {
      timeSuffix = inputList.get(i);
      props.put("TimeSuffix", timeSuffix);
      DiscoveryStepAbstract im = new ImportLogFile(this.props, this.es, this.spark);
      im.execute();
    }

    LOG.info("Web log ingest complete.");

  }

  /**
   * Method of reconstructing user sessions from raw web logs
   */
  public void sessionRestruct() {
    LOG.info("Starting Session reconstruction.");
    ArrayList<String> inputList = (ArrayList<String>) getFileList(props.getProperty(MudrodConstants.DATA_DIR));
    for (int i = 0; i < inputList.size(); i++) {
      timeSuffix = inputList.get(i); // change timeSuffix dynamically
      props.put(MudrodConstants.TIME_SUFFIX, timeSuffix);
      DiscoveryStepAbstract cd = new CrawlerDetection(this.props, this.es, this.spark);
      cd.execute();

      DiscoveryStepAbstract sg = new SessionGenerator(this.props, this.es, this.spark);
      sg.execute();

      DiscoveryStepAbstract ss = new SessionStatistic(this.props, this.es, this.spark);
      ss.execute();

      DiscoveryStepAbstract rr = new RemoveRawLog(this.props, this.es, this.spark);
      rr.execute();

      endTime = System.currentTimeMillis();
    }
    LOG.info("Session reconstruction complete.");
  }

  @Override
  public void process() {
    LOG.info("Starting Web log processing.");
    startTime = System.currentTimeMillis();

    DiscoveryStepAbstract svd = new ClickStreamAnalyzer(this.props, this.es, this.spark);
    svd.execute();

    DiscoveryStepAbstract ua = new UserHistoryAnalyzer(this.props, this.es, this.spark);
    ua.execute();

    endTime = System.currentTimeMillis();
    LOG.info("Web log processing complete. Time elaspsed {} seconds.", (endTime - startTime) / 1000);
  }

  @Override
  public void output() {
    // not implemented yet!
  }
}
