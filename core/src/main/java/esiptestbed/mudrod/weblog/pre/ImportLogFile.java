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
package esiptestbed.mudrod.weblog.pre;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaRDD;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.weblog.structure.ApacheAccessLog;
import esiptestbed.mudrod.weblog.structure.FtpLog;

/**
 * Supports ability to parse and process FTP and HTTP log files
 */
public class ImportLogFile extends DiscoveryStepAbstract {

  private static final Logger LOG = LoggerFactory
      .getLogger(ImportLogFile.class);

  private static final String TIME_SUFFIX = "TimeSuffix";

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  String logEntryPattern = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] "
      + "\"(.+?)\" (\\d{3}) (\\d+|-) \"((?:[^\"]|\")+)\" \"([^\"]+)\"";

  public static final int NUM_FIELDS = 9;
  Pattern p = Pattern.compile(logEntryPattern);
  Matcher matcher;

  /**
   * Constructor supporting a number of parameters documented below.
   * 
   * @param props
   *          a {@link java.util.Map} containing K,V of type String, String
   *          respectively.
   * @param es
   *          the {@link esiptestbed.mudrod.driver.ESDriver} used to persist log
   *          files.
   * @param spark
   *          the {@link esiptestbed.mudrod.driver.SparkDriver} used to process
   *          input log files.
   */
  public ImportLogFile(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  @Override
  public Object execute() {
    LOG.info("*****************Import starts******************");
    startTime = System.currentTimeMillis();
    readFile();
    endTime = System.currentTimeMillis();
    LOG.info("*****************Import ends******************Took {}s",
        (endTime - startTime) / 1000);
    es.refreshIndex();
    return null;
  }

  /**
   * Utility function to aid String to Number formatting such that three letter
   * months such as 'Jan' are converted to the Gregorian integer equivalent.
   * 
   * @param time
   *          the input {@link java.lang.String} to convert to int.
   * @return the converted Month as an int.
   */
  public String SwitchtoNum(String time) {
    if (time.contains("Jan")) {
      time = time.replace("Jan", "1");
    } else if (time.contains("Feb")) {
      time = time.replace("Feb", "2");
    } else if (time.contains("Mar")) {
      time = time.replace("Mar", "3");
    } else if (time.contains("Apr")) {
      time = time.replace("Apr", "4");
    } else if (time.contains("May")) {
      time = time.replace("May", "5");
    } else if (time.contains("Jun")) {
      time = time.replace("Jun", "6");
    } else if (time.contains("Jul")) {
      time = time.replace("Jul", "7");
    } else if (time.contains("Aug")) {
      time = time.replace("Aug", "8");
    } else if (time.contains("Sep")) {
      time = time.replace("Sep", "9");
    } else if (time.contains("Oct")) {
      time = time.replace("Oct", "10");
    } else if (time.contains("Nov")) {
      time = time.replace("Nov", "11");
    } else if (time.contains("Dec")) {
      time = time.replace("Dec", "12");
    }
    return time;
  }

  /**
   * Read the FTP or HTTP log path with the intention of processing lines from
   * log files.
   */
  public void readFile() {
    String httplogpath = props.getProperty("logDir")
        + props.getProperty("httpPrefix") + props.getProperty(TIME_SUFFIX) + "/"
        + props.getProperty("httpPrefix") + props.getProperty(TIME_SUFFIX);

    String ftplogpath = props.getProperty("logDir")
        + props.getProperty("ftpPrefix") + props.getProperty(TIME_SUFFIX) + "/"
        + props.getProperty("ftpPrefix") + props.getProperty(TIME_SUFFIX);

    importHttpfile(httplogpath);
    importFtpfile(ftplogpath);

    /* es.createBulkProcesser();
    try {
      readLogFile(httplogpath, "http", props.getProperty("indexName"),
          this.httpType);
      readLogFile(ftplogpath, "FTP", props.getProperty("indexName"),
          this.ftpType);
    
    } catch (IOException e) {
      LOG.error("Error whilst reading log file.", e);
    }
    es.destroyBulkProcessor();*/
  }

  /*  public void importHttpfile(String httplogpath) {
    // import http logs
    JavaRDD<ApacheAccessLog> accessLogs = spark.sc.textFile(httplogpath)
        .map(s -> ApacheAccessLog.parseFromLogLine(s))
        .filter(ApacheAccessLog::checknull);
    JavaEsSpark.saveToEs(accessLogs,
        props.getProperty("indexName") + "/" + this.httpType);
  }
  
  public void importFtpfile(String ftplogpath) {
    // import ftp logs
    JavaRDD<FtpLog> ftpLogs = spark.sc.textFile(ftplogpath)
        .map(s -> FtpLog.parseFromLogLine(s)).filter(FtpLog::checknull);
  
    JavaEsSpark.saveToEs(ftpLogs,
        props.getProperty("indexName") + "/" + this.ftpType);
  }*/

  public void importHttpfile(String httplogpath) {
    // import http logs
    JavaRDD<String> accessLogs = spark.sc.textFile(httplogpath)
        .map(s -> ApacheAccessLog.parseFromLogLine(s))
        .filter(ApacheAccessLog::checknull);
    JavaEsSpark.saveJsonToEs(accessLogs,
        props.getProperty("indexName") + "/" + this.httpType);
  }

  public void importFtpfile(String ftplogpath) {
    // import ftp logs
    JavaRDD<String> ftpLogs = spark.sc.textFile(ftplogpath)
        .map(s -> FtpLog.parseFromLogLine(s)).filter(FtpLog::checknull);

    JavaEsSpark.saveJsonToEs(ftpLogs,
        props.getProperty("indexName") + "/" + this.ftpType);
  }

  /**
   * Process a log path on local file system which contains the relevant
   * parameters as below.
   * 
   * @param fileName
   *          the {@link java.lang.String} path to the log directory on file
   *          system
   * @param protocol
   *          whether to process 'http' or 'FTP'
   * @param index
   *          the index name to write logs to
   * @param type
   *          either one of
   *          {@link esiptestbed.mudrod.discoveryengine.MudrodAbstract#ftpType}
   *          or
   *          {@link esiptestbed.mudrod.discoveryengine.MudrodAbstract#httpType}
   * @throws IOException
   *           if there is an error reading anything from the fileName provided.
   */
  public void readLogFile(String fileName, String protocol, String index,
      String type) throws IOException {
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
      LOG.info("Num of {}: {}", protocol, count);
    }
  }

  /**
   * Parse a single FTP log entry
   * 
   * @param log
   *          a single log line
   * @param index
   *          the index name we wish to persist the log line to
   * @param type
   *          either one of
   *          {@link esiptestbed.mudrod.discoveryengine.MudrodAbstract#ftpType}
   *          or
   *          {@link esiptestbed.mudrod.discoveryengine.MudrodAbstract#httpType}
   */
  public void parseSingleLineFTP(String log, String index, String type) {
    String ip = log.split(" +")[6];

    String time = log.split(" +")[1] + ":" + log.split(" +")[2] + ":"
        + log.split(" +")[3] + ":" + log.split(" +")[4];

    time = SwitchtoNum(time);
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
            .source(jsonBuilder().startObject().field("LogType", "ftp")
                .field("IP", ip).field("Time", date).field("Request", request)
                .field("Bytes", Long.parseLong(bytes)).endObject());
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
   * @param log
   *          a single log line
   * @param index
   *          the index name we wish to persist the log line to
   * @param type
   *          either one of
   *          {@link esiptestbed.mudrod.discoveryengine.MudrodAbstract#ftpType}
   *          or
   *          {@link esiptestbed.mudrod.discoveryengine.MudrodAbstract#httpType}
   */
  public void parseSingleLineHTTP(String log, String index, String type) {
    matcher = p.matcher(log);
    if (!matcher.matches() || NUM_FIELDS != matcher.groupCount()) {
      return;
    }
    String time = matcher.group(4);
    time = SwitchtoNum(time);
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
    CrawlerDetection crawlerDe = new CrawlerDetection(this.props, this.es,
        this.spark);
    if (!crawlerDe.checkKnownCrawler(agent)) {
      boolean tag = false;
      String[] mimeTypes = { ".js", ".css", ".jpg", ".png", ".ico",
          "image_captcha", "autocomplete", ".gif", "/alldata/", "/api/",
          "get / http/1.1", ".jpeg", "/ws/" };
      for (int i = 0; i < mimeTypes.length; i++) {
        if (request.contains(mimeTypes[i])) {
          tag = true;
          break;
        }
      }

      if (tag == false) {
        IndexRequest ir = null;
        executeBulkRequest(ir, index, type, matcher, date, bytes);
      }
    }
  }

  private void executeBulkRequest(IndexRequest ir, String index, String type,
      Matcher matcher, Date date, String bytes) {
    try {
      ir = new IndexRequest(index, type).source(jsonBuilder().startObject()
          .field("LogType", "PO.DAAC").field("IP", matcher.group(1))
          .field("Time", date).field("Request", matcher.group(5))
          .field("Response", matcher.group(6))
          .field("Bytes", Integer.parseInt(bytes))
          .field("Referer", matcher.group(8)).field("Browser", matcher.group(9))
          .endObject());

      es.getBulkProcessor().add(ir);
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
