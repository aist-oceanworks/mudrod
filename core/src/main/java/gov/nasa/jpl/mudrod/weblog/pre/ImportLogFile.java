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
package gov.nasa.jpl.mudrod.weblog.pre;

import gov.nasa.jpl.mudrod.driver.ESDriver;
import gov.nasa.jpl.mudrod.driver.SparkDriver;
import gov.nasa.jpl.mudrod.main.MudrodConstants;
import gov.nasa.jpl.mudrod.weblog.structure.ApacheAccessLog;
import gov.nasa.jpl.mudrod.weblog.structure.FtpLog;
import org.apache.spark.api.java.JavaRDD;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Supports ability to parse and process FTP and HTTP log files
 */
public class ImportLogFile extends LogAbstract {

  private static final Logger LOG = LoggerFactory.getLogger(ImportLogFile.class);

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  String logEntryPattern = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] " + "\"(.+?)\" (\\d{3}) (\\d+|-) \"((?:[^\"]|\")+)\" \"([^\"]+)\"";

  public static final int NUM_FIELDS = 9;
  Pattern p = Pattern.compile(logEntryPattern);
  transient Matcher matcher;

  /**
   * Constructor supporting a number of parameters documented below.
   *
   * @param props a {@link java.util.Map} containing K,V of type String, String
   *              respectively.
   * @param es    the {@link ESDriver} used to persist log
   *              files.
   * @param spark the {@link SparkDriver} used to process
   *              input log files.
   */
  public ImportLogFile(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  @Override
  public Object execute() {
    LOG.info("Starting Log Import {}", props.getProperty(MudrodConstants.TIME_SUFFIX));
    startTime = System.currentTimeMillis();
    readFile();
    endTime = System.currentTimeMillis();
    LOG.info("Log Import complete. Time elapsed {} seconds", (endTime - startTime) / 1000);
    es.refreshIndex();
    return null;
  }

  /**
   * Utility function to aid String to Number formatting such that three letter
   * months such as 'Jan' are converted to the Gregorian integer equivalent.
   *
   * @param time the input {@link java.lang.String} to convert to int.
   * @return the converted Month as an int.
   */
  public String switchtoNum(String time) {
    String newTime = time;
    if (newTime.contains("Jan")) {
      newTime = newTime.replace("Jan", "1");
    } else if (newTime.contains("Feb")) {
      newTime = newTime.replace("Feb", "2");
    } else if (newTime.contains("Mar")) {
      newTime = newTime.replace("Mar", "3");
    } else if (newTime.contains("Apr")) {
      newTime = newTime.replace("Apr", "4");
    } else if (newTime.contains("May")) {
      newTime = newTime.replace("May", "5");
    } else if (newTime.contains("Jun")) {
      newTime = newTime.replace("Jun", "6");
    } else if (newTime.contains("Jul")) {
      newTime = newTime.replace("Jul", "7");
    } else if (newTime.contains("Aug")) {
      newTime = newTime.replace("Aug", "8");
    } else if (newTime.contains("Sep")) {
      newTime = newTime.replace("Sep", "9");
    } else if (newTime.contains("Oct")) {
      newTime = newTime.replace("Oct", "10");
    } else if (newTime.contains("Nov")) {
      newTime = newTime.replace("Nov", "11");
    } else if (newTime.contains("Dec")) {
      newTime = newTime.replace("Dec", "12");
    }
    return newTime;
  }

  public void readFile() {

    String httplogpath = null;
    String ftplogpath = null;
    
    File directory = new File(props.getProperty(MudrodConstants.DATA_DIR));
    File[] fList = directory.listFiles();
    for (File file : fList) {
      if (file.isFile() && file.getName().contains(props.getProperty(MudrodConstants.TIME_SUFFIX))) 
      {
        if (file.getName().contains(props.getProperty(MudrodConstants.HTTP_PREFIX))) 
        {
          httplogpath = file.getAbsolutePath();
        }
        
        if (file.getName().contains(props.getProperty(MudrodConstants.FTP_PREFIX))) 
        {
          ftplogpath = file.getAbsolutePath();
        }
      }
    }
    
    if(httplogpath == null || ftplogpath == null)
    {
      LOG.error("WWW file or FTP logs cannot be found, please check your data directory.");
      return;
    }

    String processingType = props.getProperty(MudrodConstants.PROCESS_TYPE, "parallel");
    if (processingType.equals("sequential")) {
      readFileInSequential(httplogpath, ftplogpath);
    } else if (processingType.equals("parallel")) {
      readFileInParallel(httplogpath, ftplogpath);
    }
  }

  /**
   * Read the FTP or HTTP log path with the intention of processing lines from
   * log files.
   *
   * @param httplogpath path to the parent directory containing http logs
   * @param ftplogpath  path to the parent directory containing ftp logs
   */
  public void readFileInSequential(String httplogpath, String ftplogpath) {
    es.createBulkProcessor();
    try {
      readLogFile(httplogpath, "http", logIndex, httpType);
      readLogFile(ftplogpath, "FTP", logIndex, ftpType);

    } catch (IOException e) {
      LOG.error("Error whilst reading log file.", e);
    }
    es.destroyBulkProcessor();
  }

  /**
   * Read the FTP or HTTP log path with the intention of processing lines from
   * log files.
   *
   * @param httplogpath path to the parent directory containing http logs
   * @param ftplogpath  path to the parent directory containing ftp logs
   */
  public void readFileInParallel(String httplogpath, String ftplogpath) {

    importHttpfile(httplogpath);
    importFtpfile(ftplogpath);
  }

  public void importHttpfile(String httplogpath) {
    // import http logs
    JavaRDD<String> accessLogs = spark.sc.textFile(httplogpath, this.partition).map(s -> ApacheAccessLog.parseFromLogLine(s)).filter(ApacheAccessLog::checknull);

    JavaEsSpark.saveJsonToEs(accessLogs, logIndex + "/" + this.httpType);
  }

  public void importFtpfile(String ftplogpath) {
    // import ftp logs
    JavaRDD<String> ftpLogs = spark.sc.textFile(ftplogpath, this.partition).map(s -> FtpLog.parseFromLogLine(s)).filter(FtpLog::checknull);

    JavaEsSpark.saveJsonToEs(ftpLogs, logIndex + "/" + this.ftpType);
  }

  /**
   * Process a log path on local file system which contains the relevant
   * parameters as below.
   *
   * @param fileName the {@link java.lang.String} path to the log directory on file
   *                 system
   * @param protocol whether to process 'http' or 'FTP'
   * @param index    the index name to write logs to
   * @param type     one of the available protocols from which Mudrod logs are obtained.
   * @throws IOException if there is an error reading anything from the fileName provided.
   */
  public void readLogFile(String fileName, String protocol, String index, String type) throws IOException {
    BufferedReader br = new BufferedReader(new FileReader(fileName));
    int count = 0;
    try {
      String line = br.readLine();
      while (line != null) {
        if ("FTP".equals(protocol)) {
          parseSingleLineFTP(line, index, type);
        } else {
          parseSingleLineHTTP(line, index, type);
        }
        line = br.readLine();
        count++;
      }
    } catch (FileNotFoundException e) {
      LOG.error("File not found.", e);
    } catch (IOException e) {
      LOG.error("Error reading input directory.", e);
    } finally {
      br.close();
      LOG.info("Num of {} entries:\t{}", protocol, count);
    }
  }

  /**
   * Parse a single FTP log entry
   *
   * @param log   a single log line
   * @param index the index name we wish to persist the log line to
   * @param type  one of the available protocols from which Mudrod logs are obtained.
   */
  public void parseSingleLineFTP(String log, String index, String type) {
    String ip = log.split(" +")[6];

    String time = log.split(" +")[1] + ":" + log.split(" +")[2] + ":" + log.split(" +")[3] + ":" + log.split(" +")[4];

    time = switchtoNum(time);
    SimpleDateFormat formatter = new SimpleDateFormat("MM:dd:HH:mm:ss:yyyy");
    Date date = null;
    try {
      date = formatter.parse(time);
    } catch (ParseException e) {
      LOG.error("Error whilst parsing the date.", e);
    }
    String bytes = log.split(" +")[7];

    String request = log.split(" +")[8].toLowerCase();

    if (!request.contains("/misc/") && !request.contains("readme")) {
      IndexRequest ir;
      try {
        ir = new IndexRequest(index, type)
            .source(jsonBuilder().startObject().field("LogType", "ftp").field("IP", ip).field("Time", date).field("Request", request).field("Bytes", Long.parseLong(bytes)).endObject());
        es.getBulkProcessor().add(ir);
      } catch (NumberFormatException e) {
        LOG.error("Error whilst processing numbers", e);
      } catch (IOException e) {
        LOG.error("IOError whilst adding to the bulk processor.", e);
      }
    }

  }

  /**
   * Parse a single HTTP log entry
   *
   * @param log   a single log line
   * @param index the index name we wish to persist the log line to
   * @param type  one of the available protocols from which Mudrod logs are obtained.
   */
  public void parseSingleLineHTTP(String log, String index, String type) {
    matcher = p.matcher(log);
    if (!matcher.matches() || NUM_FIELDS != matcher.groupCount()) {
      return;
    }
    String time = matcher.group(4);
    time = switchtoNum(time);
    SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
    Date date = null;
    try {
      date = formatter.parse(time);
    } catch (ParseException e) {
      LOG.error("Error whilst attempting to parse date.", e);
    }

    String bytes = matcher.group(7);
    if ("-".equals(bytes)) {
      bytes = "0";
    }

    String request = matcher.group(5).toLowerCase();
    String agent = matcher.group(9);
    CrawlerDetection crawlerDe = new CrawlerDetection(this.props, this.es, this.spark);
    if (!crawlerDe.checkKnownCrawler(agent)) {
      boolean tag = false;
      String[] mimeTypes = { ".js", ".css", ".jpg", ".png", ".ico", "image_captcha", "autocomplete", ".gif", "/alldata/", "/api/", "get / http/1.1", ".jpeg", "/ws/" };
      for (int i = 0; i < mimeTypes.length; i++) {
        if (request.contains(mimeTypes[i])) {
          tag = true;
          break;
        }
      }

      if (!tag) {
        IndexRequest ir = null;
        executeBulkRequest(ir, index, type, matcher, date, bytes);
      }
    }
  }

  private void executeBulkRequest(IndexRequest ir, String index, String type, Matcher matcher, Date date, String bytes) {
    IndexRequest newIr = ir;
    try {
      newIr = new IndexRequest(index, type).source(
          jsonBuilder().startObject().field("LogType", "PO.DAAC").field("IP", matcher.group(1)).field("Time", date).field("Request", matcher.group(5)).field("Response", matcher.group(6))
              .field("Bytes", Integer.parseInt(bytes)).field("Referer", matcher.group(8)).field("Browser", matcher.group(9)).endObject());

      es.getBulkProcessor().add(newIr);
    } catch (NumberFormatException e) {
      LOG.error("Error whilst processing numbers", e);
    } catch (IOException e) {
      LOG.error("IOError whilst adding to the bulk processor.", e);
    }
  }

  @Override
  public Object execute(Object o) {
    return null;
  }
}
