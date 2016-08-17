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
package esiptestbed.mudrod.main;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

import esiptestbed.mudrod.discoveryengine.DiscoveryEngineAbstract;
import esiptestbed.mudrod.discoveryengine.MetadataDiscoveryEngine;
import esiptestbed.mudrod.discoveryengine.OntologyDiscoveryEngine;
import esiptestbed.mudrod.discoveryengine.WeblogDiscoveryEngine;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.integration.LinkageIntegration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.org.apache.bcel.internal.generic.ReturnaddressType;

/**
 * Main entry point for Running the Mudrod system.
 * Invocation of this class is tightly linked to the primary Mudrod 
 * configuration which can be located at
 * <a href="https://github.com/mudrod/mudrod/blob/master/core/src/main/resources/config.xml">config.xml</a>.
 */
public class MudrodEngine {

  private static final Logger LOG = LoggerFactory.getLogger(MudrodEngine.class);
  private Properties props = new Properties();
  private ESDriver es = null;
  private SparkDriver spark = null;
  private static final String LOG_INGEST = "logIngest";
  private static final String FULL_INGEST = "fullIngest";
  private static final String LOG_DIR = "logDir";

  /**
   * Public constructor for this class, loads a default config.xml.
   */
  public MudrodEngine() {
    loadConfig();
  }

  public ESDriver startESDriver() {
    return new ESDriver(props);
  }

  public SparkDriver startSparkDriver() {
    return new SparkDriver();
  }

  /**
   * Retreive the Mudrod configuration as a Properties Map
   * containing K, V of type String.
   * @return a {@link java.util.Properties} object
   */
  public Properties getConfig() {
    return props;
  }

  /**
   * Retreive the Mudrod {@link esiptestbed.mudrod.driver.ESDriver}
   * @return the {@link esiptestbed.mudrod.driver.ESDriver} instance.
   */
  public ESDriver getES() {
    return this.es;
  }

  /**
   * Load the configuration provided at
   * <a href="https://github.com/mudrod/mudrod/blob/master/core/src/main/resources/config.xml">config.xml</a>.
   * @return a populated {@link java.util.Properties} object.
   */
  public Properties loadConfig() {
    SAXBuilder saxBuilder = new SAXBuilder();
    InputStream configStream = MudrodEngine.class.getClassLoader()
        .getResourceAsStream("config.xml");

    Document document;
    try {
      document = saxBuilder.build(configStream);
      Element rootNode = document.getRootElement();
      List<Element> paraList = rootNode.getChildren("para");

      for (int i = 0; i < paraList.size(); i++) {
        Element paraNode = paraList.get(i);
        props.put(paraNode.getAttributeValue("name"),
            paraNode.getTextTrim());
      }
    } catch (JDOMException | IOException e) {
      LOG.error("Exception whilst retreiving or processing XML contained within 'config.xml'!", e);
    }
    return getConfig();

  }

  /**
   * Preprocess and process various {@link esiptestbed.mudrod.discoveryengine.DiscoveryEngineAbstract}
   * implementations for weblog, ontology and metadata, linkage discovery and integration.
   */
  public void start() {
    DiscoveryEngineAbstract wd = new WeblogDiscoveryEngine(props, es, spark);
    wd.preprocess();
    wd.process();

    DiscoveryEngineAbstract od = new OntologyDiscoveryEngine(props, es, spark);
    od.preprocess();
    od.process();

    DiscoveryEngineAbstract md = new MetadataDiscoveryEngine(props, es, spark);
    md.preprocess();
    md.process();

    LinkageIntegration li = new LinkageIntegration(props, es, spark);
    li.execute();
  }

  /**
   * Only preprocess various {@link esiptestbed.mudrod.discoveryengine.DiscoveryEngineAbstract}
   * implementations for weblog, ontology and metadata, linkage discovery and integration.
   */
  public void startProcessing() {
    DiscoveryEngineAbstract wd = new WeblogDiscoveryEngine(props, es, spark);
    wd.process();

    DiscoveryEngineAbstract od = new OntologyDiscoveryEngine(props, es, spark);
    od.preprocess();
    od.process();

    DiscoveryEngineAbstract md = new MetadataDiscoveryEngine(props, es, spark);
    md.preprocess();
    md.process();

    LinkageIntegration li = new LinkageIntegration(props, es, spark);
    li.execute();
  }

  /**
   * Begin ingesting logs with the {@link esiptestbed.mudrod.discoveryengine.WeblogDiscoveryEngine}
   */
  public void startLogIngest() {
    WeblogDiscoveryEngine wd = new WeblogDiscoveryEngine(props, es, spark);
    wd.logIngest();
  }

  /**
   * Close the connection to the {@link esiptestbed.mudrod.driver.ESDriver} instance.
   */
  public void end() {
    es.close();
  }

  /**
   * Main program invocation. Accepts one argument denoting location (on disk)
   * to a log file which is to be ingested. Help will be provided if invoked
   * with incorrect parameters.
   * @param args {@link java.lang.String} array contaning correct parameters.
   */
  public static void main(String[] args) {
    // boolean options
    Option helpOpt = new Option("h", "help", false, "show this help message");
    Option logIngestOpt = new Option("l", LOG_INGEST, false, "begin log ingest with the WeblogDiscoveryEngine only");
    Option fullIngestOpt = new Option("f", FULL_INGEST, false, "begin full ingest Mudrod workflow");
    // argument options
    Option logDirOpt = Option.builder(LOG_DIR).required(true).numberOfArgs(1)
        .hasArg(true).desc("the log directory to be processed by Mudrod").argName(LOG_DIR).build();

    // create the options
    Options options = new Options();
    options.addOption(helpOpt);
    options.addOption(logIngestOpt);
    options.addOption(fullIngestOpt);
    options.addOption(logDirOpt);

    CommandLineParser parser = new DefaultParser();
    try {
      CommandLine line = parser.parse(options, args);
      String processingType;
      if (line.hasOption(LOG_INGEST)) {
        processingType = LOG_INGEST;
      } else {
        processingType = FULL_INGEST;
      }
      String dataDir = line.getOptionValue(LOG_DIR).replace("\\", "/");
      if(!dataDir.endsWith("/")){
        dataDir += "/";
      }

      MudrodEngine me = new MudrodEngine();
      me.props.put(LOG_DIR, dataDir);

      switch (processingType) {
      case LOG_INGEST:
        me.startLogIngest();
        break;
      case FULL_INGEST:
        loadFullConfig(me, dataDir);
        me.startProcessing(); 
        break;
      default:
        break;
      }
      me.end();
    } catch (Exception e) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("MudrodEngine: 'logDir' argument is mandatory. "
          + "User must also provide either 'logIngest' or 'fullIngest'.", options, true);
      LOG.error("MudrodEngine: 'logDir' argument is mandatory. "
          + "User must also provide either 'logIngest' or 'fullIngest'.", e);
      return;
    }
  }

  private static void loadFullConfig(MudrodEngine me, String dataDir) {
    me.props.put("ontologyInputDir", dataDir + "SWEET_ocean/");
    me.props.put("oceanTriples", dataDir + "Ocean_triples.csv");
    me.props.put("userHistoryMatrix", dataDir + "UserHistoryMatrix.csv");
    me.props.put("clickstreamMatrix", dataDir + "ClickstreamMatrix.csv");
    me.props.put("metadataMatrix", dataDir + "MetadataMatrix.csv");
    me.props.put("clickstreamSVDMatrix_tmp", dataDir + "clickstreamSVDMatrix_tmp.csv");
    me.props.put("metadataSVDMatrix_tmp", dataDir + "metadataSVDMatrix_tmp.csv");
    me.props.put("raw_metadataPath", dataDir + "RawMetadata");
  }
}
