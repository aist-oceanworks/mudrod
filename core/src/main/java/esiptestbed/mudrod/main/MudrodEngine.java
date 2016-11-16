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
import java.util.List;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import esiptestbed.mudrod.discoveryengine.DiscoveryEngineAbstract;
import esiptestbed.mudrod.discoveryengine.MetadataDiscoveryEngine;
import esiptestbed.mudrod.discoveryengine.OntologyDiscoveryEngine;
import esiptestbed.mudrod.discoveryengine.RecommendEngine;
import esiptestbed.mudrod.discoveryengine.WeblogDiscoveryEngine;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.integration.LinkageIntegration;

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

  /**
   * Public constructor for this class.
   */
  public MudrodEngine() {
    // default constructor
  }

  /**
   * Start the {@link esiptestbed.mudrod.driver.ESDriver}. Should only be called
   * after call to {@link esiptestbed.mudrod.main.MudrodEngine#loadConfig()}
   * 
   * @return fully provisioned {@link esiptestbed.mudrod.driver.ESDriver}
   */
  public ESDriver startESDriver() {
    return new ESDriver(props);
  }

  /**
   * Start the {@link esiptestbed.mudrod.driver.SparkDriver}. Should only be
   * called after call to
   * {@link esiptestbed.mudrod.main.MudrodEngine#loadConfig()}
   * 
   * @return fully provisioned {@link esiptestbed.mudrod.driver.SparkDriver}
   */
  public SparkDriver startSparkDriver() {
    return new SparkDriver();
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
   * Retreive the Mudrod {@link esiptestbed.mudrod.driver.ESDriver}
   * 
   * @return the {@link esiptestbed.mudrod.driver.ESDriver} instance.
   */
  public ESDriver getESDriver() {
    return this.es;
  }

  /**
   * Set the Elasticsearch driver for MUDROD
   * 
   * @param es
   *          an ES driver instance
   */
  public void setESDriver(ESDriver es) {
    this.es = es;
  }

  /**
   * Load the configuration provided at <a href=
   * "https://github.com/mudrod/mudrod/blob/master/core/src/main/resources/config.xml">config.xml</a>.
   * 
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
   * {@link esiptestbed.mudrod.discoveryengine.DiscoveryEngineAbstract}
   * implementations for weblog, ontology and metadata, linkage discovery and
   * integration.
   */
  public void startFullIngest() {
    /*DiscoveryEngineAbstract wd = new WeblogDiscoveryEngine(props, es, spark);
    wd.preprocess();
    wd.process();
    
    DiscoveryEngineAbstract od = new OntologyDiscoveryEngine(props, es, spark);
    od.preprocess();
    od.process();
    
    DiscoveryEngineAbstract md = new MetadataDiscoveryEngine(props, es, spark);
    md.preprocess();
    md.process();
    
    LinkageIntegration li = new LinkageIntegration(props, es, spark);
    li.execute();*/

    DiscoveryEngineAbstract recom = new RecommendEngine(props, es, spark);
    // recom.preprocess();
    recom.process();
  }

  /**
   * Begin ingesting logs with the
   * {@link esiptestbed.mudrod.discoveryengine.WeblogDiscoveryEngine}
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
   * {@link esiptestbed.mudrod.discoveryengine.DiscoveryEngineAbstract}
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
   * {@link esiptestbed.mudrod.discoveryengine.WeblogDiscoveryEngine}
   */
  public void startLogIngest() {
    WeblogDiscoveryEngine wd = new WeblogDiscoveryEngine(props, es, spark);
    wd.logIngest();
  }

  /**
   * Close the connection to the {@link esiptestbed.mudrod.driver.ESDriver}
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
   * @param args
   *          {@link java.lang.String} array contaning correct parameters.
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
    Option logDirOpt = Option.builder(LOG_DIR).required(true).numberOfArgs(1)
        .hasArg(true).desc("the log directory to be processed by Mudrod")
        .argName(LOG_DIR).build();

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

    CommandLineParser parser = new DefaultParser();
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
      me.es = new ESDriver(me.getConfig());
      me.spark = new SparkDriver();
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
      LOG.error("Error inputting command line!", e);
      return;
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
    me.props.put("metadataSVDMatrix_tmp",
        dataDir + "metadataSVDMatrix_tmp.csv");
    me.props.put("raw_metadataPath", dataDir + "RawMetadata");

    me.props.put("jtopia", dataDir + "jtopiaModel");
    me.props.put("metadata_term_tfidf_matrix",
        dataDir + "metadata_term_tfidf.csv");
    me.props.put("metadata_word_tfidf_matrix",
        dataDir + "metadata_word_tfidf.csv");
    me.props.put("session_metadata_Matrix",
        dataDir + "metadata_session_coocurrence_matrix.csv");

    me.props.put("metadataOBCode", dataDir + "MetadataOHCode");
    me.props.put("metadata_topic", dataDir + "metadata_topic");
    me.props.put("metadata_topic_matrix",
        dataDir + "metadata_topic_matrix.csv");
  }

  /**
   * Obtain the spark implementation.
   * 
   * @return the {@link esiptestbed.mudrod.driver.SparkDriver}
   */
  public SparkDriver getSparkDriver() {
    return this.spark;
  }

  /**
   * Set the {@link esiptestbed.mudrod.driver.SparkDriver}
   * 
   * @param sparkDriver
   *          a configured {@link esiptestbed.mudrod.driver.SparkDriver}
   */
  public void setSparkDriver(SparkDriver sparkDriver) {
    this.spark = sparkDriver;

  }
}
