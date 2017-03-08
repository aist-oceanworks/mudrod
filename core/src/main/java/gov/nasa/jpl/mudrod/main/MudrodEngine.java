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
package gov.nasa.jpl.mudrod.main;

import gov.nasa.jpl.mudrod.discoveryengine.*;
import gov.nasa.jpl.mudrod.driver.ESDriver;
import gov.nasa.jpl.mudrod.driver.SparkDriver;
import gov.nasa.jpl.mudrod.integration.LinkageIntegration;
import org.apache.commons.cli.*;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

/**
 * Main entry point for Running the Mudrod system. Invocation of this class is
 * tightly linked to the primary Mudrod configuration which can be located at
 * <a href=
 * "https://github.com/mudrod/mudrod/blob/master/core/src/main/resources/config.xml">config.xml</a>.
 */
public class MudrodEngine {

  private static final Logger LOG = LoggerFactory.getLogger(MudrodEngine.class);
  private Properties props = new Properties();
  private ESDriver es = null;
  private SparkDriver spark = null;
  private static final String LOG_INGEST = "logIngest";
  private static final String FULL_INGEST = "fullIngest";
  private static final String PROCESSING = "processingWithPreResults";
  private static final String SESSION_RECON = "sessionReconstruction";
  private static final String VOCAB_SIM_FROM_LOG = "vocabSimFromLog";
  private static final String ADD_META_ONTO = "addSimFromMetadataAndOnto";
  private static final String LOG_DIR = "logDir";
  private static final String ES_HOST = "esHost";
  private static final String ES_TCP_PORT = "esTCPPort";
  private static final String ES_HTTP_PORT = "esPort";

  /**
   * Public constructor for this class.
   */
  public MudrodEngine() {
    // default constructor
  }

  /**
   * Start the {@link ESDriver}. Should only be called
   * after call to {@link MudrodEngine#loadConfig()}
   *
   * @return fully provisioned {@link ESDriver}
   */
  public ESDriver startESDriver() {
    return new ESDriver(props);
  }

  /**
   * Start the {@link SparkDriver}. Should only be
   * called after call to
   * {@link MudrodEngine#loadConfig()}
   *
   * @return fully provisioned {@link SparkDriver}
   */
  public SparkDriver startSparkDriver() {
    return new SparkDriver(props);
  }

  /**
   * Retreive the Mudrod configuration as a Properties Map containing K, V of
   * type String.
   *
   * @return a {@link java.util.Properties} object
   */
  public Properties getConfig() {
    return props;
  }

  /**
   * Retreive the Mudrod {@link ESDriver}
   *
   * @return the {@link ESDriver} instance.
   */
  public ESDriver getESDriver() {
    return this.es;
  }

  /**
   * Set the Elasticsearch driver for MUDROD
   *
   * @param es an ES driver instance
   */
  public void setESDriver(ESDriver es) {
    this.es = es;
  }

  private InputStream locateConfig() {

    String configLocation =
        System.getenv(MudrodConstants.MUDROD_CONFIG) == null ?
            "" :
            System.getenv(MudrodConstants.MUDROD_CONFIG);
    File configFile = new File(configLocation);

    try {
      InputStream configStream = new FileInputStream(configFile);
      LOG.info("Loaded config file from " + configFile.getAbsolutePath());
      return configStream;
    } catch (IOException e) {
      LOG.info("File specified by environment variable "
          + MudrodConstants.MUDROD_CONFIG + "=\'" + configLocation
          + "\' could not be loaded. " + e.getMessage());
    }

    InputStream configStream = MudrodEngine.class.getClassLoader()
        .getResourceAsStream("config.xml");

    if (configStream != null) {
      LOG.info("Loaded config file from " + MudrodEngine.class.getClassLoader()
          .getResource("config.xml").getPath());
    }

    return configStream;
  }

  /**
   * Load the configuration provided at <a href=
   * "https://github.com/mudrod/mudrod/blob/master/core/src/main/resources/config.xml">config.xml</a>.
   *
   * @return a populated {@link java.util.Properties} object.
   */
  public Properties loadConfig() {
    SAXBuilder saxBuilder = new SAXBuilder();

    InputStream configStream = locateConfig();

    Document document;
    try {
      document = saxBuilder.build(configStream);
      Element rootNode = document.getRootElement();
      List<Element> paraList = rootNode.getChildren("para");

      for (int i = 0; i < paraList.size(); i++) {
        Element paraNode = paraList.get(i);
        props.put(paraNode.getAttributeValue("name"), paraNode.getTextTrim());
      }
    } catch (JDOMException | IOException e) {
      LOG.error(
          "Exception whilst retreiving or processing XML contained within 'config.xml'!",
          e);
    }
    return getConfig();

  }

  /**
   * Preprocess and process various
   * {@link DiscoveryEngineAbstract}
   * implementations for weblog, ontology and metadata, linkage discovery and
   * integration.
   */
  public void startFullIngest() {
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

    DiscoveryEngineAbstract recom = new RecommendEngine(props, es, spark);
    recom.preprocess();
    recom.process();
  }

  /**
   * Begin ingesting logs with the
   * {@link WeblogDiscoveryEngine}
   */
  public void logIngest() {
    WeblogDiscoveryEngine wd = new WeblogDiscoveryEngine(props, es, spark);
    wd.logIngest();
  }

  /**
   * Reconstructing user sessions based on raw logs.
   */
  public void sessionRestruction() {
    WeblogDiscoveryEngine wd = new WeblogDiscoveryEngine(props, es, spark);
    wd.sessionRestruct();
  }

  /**
   * Calculating vocab similarity based on reconstructed sessions.
   */
  public void vocabSimFromLog() {
    WeblogDiscoveryEngine wd = new WeblogDiscoveryEngine(props, es, spark);
    wd.process();
  }

  /**
   * Adding ontology and metadata results to vocab similarity results from web
   * logs.
   */
  public void addMetaAndOntologySim() {
    DiscoveryEngineAbstract od = new OntologyDiscoveryEngine(props, es, spark);
    od.preprocess();
    od.process();

    DiscoveryEngineAbstract md = new MetadataDiscoveryEngine(props, es, spark);
    md.preprocess();
    md.process();
    LOG.info("*****************Ontology and metadata similarity have "
        + "been added successfully******************");
  }

  /**
   * Only preprocess various
   * {@link DiscoveryEngineAbstract}
   * implementations for weblog, ontology and metadata, linkage discovery and
   * integration.
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

    DiscoveryEngineAbstract recom = new RecommendEngine(props, es, spark);
    recom.process();
  }

  /**
   * Begin ingesting logs with the
   * {@link WeblogDiscoveryEngine}
   */
  public void startLogIngest() {
    WeblogDiscoveryEngine wd = new WeblogDiscoveryEngine(props, es, spark);
    wd.logIngest();
  }

  /**
   * Close the connection to the {@link ESDriver}
   * instance.
   */
  public void end() {
    if (es != null) {
      es.close();
    }
  }

  /**
   * Main program invocation. Accepts one argument denoting location (on disk)
   * to a log file which is to be ingested. Help will be provided if invoked
   * with incorrect parameters.
   *
   * @param args {@link java.lang.String} array contaning correct parameters.
   */
  public static void main(String[] args) {
    // boolean options
    Option helpOpt = new Option("h", "help", false, "show this help message");

    // preprocessing + processing
    Option fullIngestOpt = new Option("f", FULL_INGEST, false,
        "begin full ingest Mudrod workflow");
    // processing only, assuming that preprocessing results is in logDir
    Option processingOpt = new Option("p", PROCESSING, false,
        "begin processing with preprocessing results");

    // import raw web log into Elasticsearch
    Option logIngestOpt = new Option("l", LOG_INGEST, false,
        "begin log ingest without any processing only");
    // preprocessing web log, assuming web log has already been imported
    Option sessionReconOpt = new Option("s", SESSION_RECON, false,
        "begin session reconstruction");
    // calculate vocab similarity from session reconstrution results
    Option vocabSimFromOpt = new Option("v", VOCAB_SIM_FROM_LOG, false,
        "begin similarity calulation from web log Mudrod workflow");
    // add metadata and ontology preprocessing and processing results into web
    // log vocab similarity
    Option addMetaOntoOpt = new Option("a", ADD_META_ONTO, false,
        "begin adding metadata and ontology results");

    // argument options
    Option logDirOpt = OptionBuilder.hasArg(true)
        .withArgName("/path/to/log/directory").hasArgs(1)
        .withDescription("the log directory to be processed by Mudrod")
        .withLongOpt("logDirectory").isRequired().create(LOG_DIR);

    Option esHostOpt = OptionBuilder.hasArg(true).withArgName("host_name")
        .hasArgs(1).withDescription("elasticsearch cluster unicast host")
        .withLongOpt("elasticSearchHost").isRequired(false).create(ES_HOST);

    Option esTCPPortOpt = OptionBuilder.hasArg(true).withArgName("port_num")
        .hasArgs(1).withDescription("elasticsearch transport TCP port")
        .withLongOpt("elasticSearchTransportTCPPort").isRequired(false)
        .create(ES_TCP_PORT);

    Option esPortOpt = OptionBuilder.hasArg(true).withArgName("port_num")
        .hasArgs(1).withDescription("elasticsearch HTTP/REST port")
        .withLongOpt("elasticSearchHTTPPort").isRequired(false)
        .create(ES_HTTP_PORT);

    // create the options
    Options options = new Options();
    options.addOption(helpOpt);
    options.addOption(logIngestOpt);
    options.addOption(fullIngestOpt);
    options.addOption(processingOpt);
    options.addOption(sessionReconOpt);
    options.addOption(vocabSimFromOpt);
    options.addOption(addMetaOntoOpt);
    options.addOption(logDirOpt);
    options.addOption(esHostOpt);
    options.addOption(esTCPPortOpt);
    options.addOption(esPortOpt);

    CommandLineParser parser = new GnuParser();
    try {
      CommandLine line = parser.parse(options, args);
      String processingType = null;

      if (line.hasOption(LOG_INGEST)) {
        processingType = LOG_INGEST;
      } else if (line.hasOption(FULL_INGEST)) {
        processingType = FULL_INGEST;
      } else if (line.hasOption(PROCESSING)) {
        processingType = PROCESSING;
      } else if (line.hasOption(SESSION_RECON)) {
        processingType = SESSION_RECON;
      } else if (line.hasOption(VOCAB_SIM_FROM_LOG)) {
        processingType = VOCAB_SIM_FROM_LOG;
      } else if (line.hasOption(ADD_META_ONTO)) {
        processingType = ADD_META_ONTO;
      }

      String dataDir = line.getOptionValue(LOG_DIR).replace("\\", "/");
      if (!dataDir.endsWith("/")) {
        dataDir += "/";
      }

      MudrodEngine me = new MudrodEngine();
      me.loadConfig();
      me.props.put(LOG_DIR, dataDir);

      if (line.hasOption(ES_HOST)) {
        String esHost = line.getOptionValue(ES_HOST);
        me.props.put(MudrodConstants.ES_UNICAST_HOSTS, esHost);
      }

      if (line.hasOption(ES_TCP_PORT)) {
        String esTcpPort = line.getOptionValue(ES_TCP_PORT);
        me.props.put(MudrodConstants.ES_TRANSPORT_TCP_PORT, esTcpPort);
      }

      if (line.hasOption(ES_HTTP_PORT)) {
        String esHttpPort = line.getOptionValue(ES_HTTP_PORT);
        me.props.put(MudrodConstants.ES_HTTP_PORT, esHttpPort);
      }

      me.es = new ESDriver(me.getConfig());
      me.spark = new SparkDriver(me.getConfig());
      loadFullConfig(me, dataDir);
      if (processingType != null) {
        switch (processingType) {
        case LOG_INGEST:
          me.logIngest();
          break;
        case PROCESSING:
          me.startProcessing();
          break;
        case SESSION_RECON:
          me.sessionRestruction();
          break;
        case VOCAB_SIM_FROM_LOG:
          me.vocabSimFromLog();
          break;
        case ADD_META_ONTO:
          me.addMetaAndOntologySim();
          break;
        case FULL_INGEST:
          me.startFullIngest();
          break;
        default:
          break;
        }
      }
      me.end();
    } catch (Exception e) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("MudrodEngine: 'logDir' argument is mandatory. "
          + "User must also provide an ingest method.", options, true);
      LOG.error("Error whilst parsing command line.", e);
    }
  }

  private static void loadFullConfig(MudrodEngine me, String dataDir) {
    me.props.put("ontologyInputDir", dataDir + "SWEET_ocean/");
    me.props.put("oceanTriples", dataDir + "Ocean_triples.csv");
    me.props.put("userHistoryMatrix", dataDir + "UserHistoryMatrix.csv");
    me.props.put("clickstreamMatrix", dataDir + "ClickstreamMatrix.csv");
    me.props.put("metadataMatrix", dataDir + "MetadataMatrix.csv");
    me.props.put("clickstreamSVDMatrix_tmp",
        dataDir + "clickstreamSVDMatrix_tmp.csv");
    me.props
        .put("metadataSVDMatrix_tmp", dataDir + "metadataSVDMatrix_tmp.csv");
    me.props.put("raw_metadataPath", dataDir + "RawMetadata");

    me.props.put("jtopia", dataDir + "jtopiaModel");
    me.props
        .put("metadata_term_tfidf_matrix", dataDir + "metadata_term_tfidf.csv");
    me.props
        .put("metadata_word_tfidf_matrix", dataDir + "metadata_word_tfidf.csv");
    me.props.put("session_metadata_Matrix",
        dataDir + "metadata_session_coocurrence_matrix.csv");

    me.props.put("metadataOBCode", dataDir + "MetadataOHCode");
    me.props.put("metadata_topic", dataDir + "metadata_topic");
    me.props
        .put("metadata_topic_matrix", dataDir + "metadata_topic_matrix.csv");
  }

  /**
   * Obtain the spark implementation.
   *
   * @return the {@link SparkDriver}
   */
  public SparkDriver getSparkDriver() {
    return this.spark;
  }

  /**
   * Set the {@link SparkDriver}
   *
   * @param sparkDriver a configured {@link SparkDriver}
   */
  public void setSparkDriver(SparkDriver sparkDriver) {
    this.spark = sparkDriver;

  }
}
