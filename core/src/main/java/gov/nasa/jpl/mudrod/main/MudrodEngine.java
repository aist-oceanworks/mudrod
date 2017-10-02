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

import gov.nasa.jpl.mudrod.discoveryengine.DiscoveryEngineAbstract;
import gov.nasa.jpl.mudrod.discoveryengine.MetadataDiscoveryEngine;
import gov.nasa.jpl.mudrod.discoveryengine.OntologyDiscoveryEngine;
import gov.nasa.jpl.mudrod.discoveryengine.RecommendEngine;
import gov.nasa.jpl.mudrod.discoveryengine.WeblogDiscoveryEngine;
import gov.nasa.jpl.mudrod.driver.ESDriver;
import gov.nasa.jpl.mudrod.driver.SparkDriver;
import gov.nasa.jpl.mudrod.integration.LinkageIntegration;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.util.List;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static gov.nasa.jpl.mudrod.main.MudrodConstants.DATA_DIR;

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
  private static final String META_INGEST = "metaIngest";
  private static final String FULL_INGEST = "fullIngest";
  private static final String PROCESSING = "processingWithPreResults";
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
   * Start the {@link ESDriver}. Should only be called after call to
   * {@link MudrodEngine#loadConfig()}
   *
   * @return fully provisioned {@link ESDriver}
   */
  public ESDriver startESDriver() {
    return new ESDriver(props);
  }

  /**
   * Start the {@link SparkDriver}. Should only be called after call to
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
   * @param es
   *          an ES driver instance
   */
  public void setESDriver(ESDriver es) {
    this.es = es;
  }

  private InputStream locateConfig() {

    String configLocation = System.getenv(MudrodConstants.MUDROD_CONFIG) == null ? "" : System.getenv(MudrodConstants.MUDROD_CONFIG);
    File configFile = new File(configLocation);

    try {
      InputStream configStream = new FileInputStream(configFile);
      LOG.info("Loaded config file from " + configFile.getAbsolutePath());
      return configStream;
    } catch (IOException e) {
      LOG.info("File specified by environment variable " + MudrodConstants.MUDROD_CONFIG + "=\'" + configLocation + "\' could not be loaded. " + e.getMessage());
    }

    InputStream configStream = MudrodEngine.class.getClassLoader().getResourceAsStream("config.xml");

    if (configStream != null) {
      LOG.info("Loaded config file from {}", MudrodEngine.class.getClassLoader().getResource("config.xml").getPath());
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
        String attributeName = paraNode.getAttributeValue("name");
        if (MudrodConstants.SVM_SGD_MODEL.equals(attributeName)) {
          props.put(attributeName, decompressSVMWithSGDModel(paraNode.getTextTrim()));
        } else {
          props.put(attributeName, paraNode.getTextTrim());
        }
      }
    } catch (JDOMException | IOException e) {
      LOG.error("Exception whilst retrieving or processing XML contained within 'config.xml'!", e);
    }
    return getConfig();

  }

  private String decompressSVMWithSGDModel(String archiveName) throws IOException {

    URL scmArchive = getClass().getClassLoader().getResource(archiveName);
    if (scmArchive == null) {
      throw new IOException("Unable to locate " + archiveName + " as a classpath resource.");
    }
    File tempDir = Files.createTempDirectory("mudrod").toFile();
    assert tempDir.setWritable(true);
    File archiveFile = new File(tempDir, archiveName);
    FileUtils.copyURLToFile(scmArchive, archiveFile);

    // Decompress archive
    int BUFFER_SIZE = 512000;
    ZipInputStream zipIn = new ZipInputStream(new FileInputStream(archiveFile));
    ZipEntry entry;
    while ((entry = zipIn.getNextEntry()) != null) {
      File f = new File(tempDir, entry.getName());
      // If the entry is a directory, create the directory.
      if (entry.isDirectory() && !f.exists()) {
        boolean created = f.mkdirs();
        if (!created) {
          LOG.error("Unable to create directory '{}', during extraction of archive contents.", f.getAbsolutePath());
        }
      } else if (!entry.isDirectory()) {
        boolean created = f.getParentFile().mkdirs();
        if (!created && !f.getParentFile().exists()) {
          LOG.error("Unable to create directory '{}', during extraction of archive contents.", f.getParentFile().getAbsolutePath());
        }
        int count;
        byte data[] = new byte[BUFFER_SIZE];
        FileOutputStream fos = new FileOutputStream(new File(tempDir, entry.getName()), false);
        try (BufferedOutputStream dest = new BufferedOutputStream(fos, BUFFER_SIZE)) {
          while ((count = zipIn.read(data, 0, BUFFER_SIZE)) != -1) {
            dest.write(data, 0, count);
          }
        }
      }
    }

    return new File(tempDir, StringUtils.removeEnd(archiveName, ".zip")).toURI().toString();
  }

  /**
   * Preprocess and process logs {@link DiscoveryEngineAbstract} implementations
   * for weblog
   */
  public void startLogIngest() {
    DiscoveryEngineAbstract wd = new WeblogDiscoveryEngine(props, es, spark);
    wd.preprocess();
    wd.process();
    LOG.info("*****************logs have been ingested successfully******************");
  }

  /**
   * updating and analysing metadata to metadata similarity results
   */
  public void startMetaIngest() {
    DiscoveryEngineAbstract md = new MetadataDiscoveryEngine(props, es, spark);
    md.preprocess();
    md.process();

    DiscoveryEngineAbstract recom = new RecommendEngine(props, es, spark);
    recom.preprocess();
    recom.process();
    LOG.info("Metadata has been ingested successfully.");
  }

  public void startFullIngest() {
    DiscoveryEngineAbstract wd = new WeblogDiscoveryEngine(props, es, spark);
    wd.preprocess();
    wd.process();

    DiscoveryEngineAbstract md = new MetadataDiscoveryEngine(props, es, spark);
    md.preprocess();
    md.process();

    DiscoveryEngineAbstract recom = new RecommendEngine(props, es, spark);
    recom.preprocess();
    recom.process();
    LOG.info("Full ingest has finished successfully.");
  }

  /**
   * Only preprocess various {@link DiscoveryEngineAbstract} implementations for
   * weblog, ontology and metadata, linkage discovery and integration.
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
   * Close the connection to the {@link ESDriver} instance.
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

    // log ingest (preprocessing + processing)
    Option logIngestOpt = new Option("l", LOG_INGEST, false, "begin log ingest");
    // metadata ingest (preprocessing + processing)
    Option metaIngestOpt = new Option("m", META_INGEST, false, "begin metadata ingest");
    // ingest both log and metadata
    Option fullIngestOpt = new Option("f", FULL_INGEST, false, "begin full ingest Mudrod workflow");
    // processing only, assuming that preprocessing results is in dataDir
    Option processingOpt = new Option("p", PROCESSING, false, "begin processing with preprocessing results");

    // argument options
    Option dataDirOpt = OptionBuilder.hasArg(true).withArgName("/path/to/data/directory").hasArgs(1).withDescription("the data directory to be processed by Mudrod").withLongOpt("dataDirectory")
        .isRequired().create(DATA_DIR);

    Option esHostOpt = OptionBuilder.hasArg(true).withArgName("host_name").hasArgs(1).withDescription("elasticsearch cluster unicast host").withLongOpt("elasticSearchHost").isRequired(false)
        .create(ES_HOST);

    Option esTCPPortOpt = OptionBuilder.hasArg(true).withArgName("port_num").hasArgs(1).withDescription("elasticsearch transport TCP port").withLongOpt("elasticSearchTransportTCPPort")
        .isRequired(false).create(ES_TCP_PORT);

    Option esPortOpt = OptionBuilder.hasArg(true).withArgName("port_num").hasArgs(1).withDescription("elasticsearch HTTP/REST port").withLongOpt("elasticSearchHTTPPort").isRequired(false)
        .create(ES_HTTP_PORT);

    // create the options
    Options options = new Options();
    options.addOption(helpOpt);
    options.addOption(logIngestOpt);
    options.addOption(metaIngestOpt);
    options.addOption(fullIngestOpt);
    options.addOption(processingOpt);
    options.addOption(dataDirOpt);
    options.addOption(esHostOpt);
    options.addOption(esTCPPortOpt);
    options.addOption(esPortOpt);

    CommandLineParser parser = new GnuParser();
    try {
      CommandLine line = parser.parse(options, args);
      String processingType = null;

      if (line.hasOption(LOG_INGEST)) {
        processingType = LOG_INGEST;
      } else if (line.hasOption(PROCESSING)) {
        processingType = PROCESSING;
      } else if (line.hasOption(META_INGEST)) {
        processingType = META_INGEST;
      } else if (line.hasOption(FULL_INGEST)) {
        processingType = FULL_INGEST;
      }

      String dataDir = line.getOptionValue(DATA_DIR).replace("\\", "/");
      if (!dataDir.endsWith("/")) {
        dataDir += "/";
      }

      MudrodEngine me = new MudrodEngine();
      me.loadConfig();
      me.props.put(DATA_DIR, dataDir);

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
        case PROCESSING:
          me.startProcessing();
          break;
        case LOG_INGEST:
          me.startLogIngest();
          break;
        case META_INGEST:
          me.startMetaIngest();
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
      formatter.printHelp("MudrodEngine: 'dataDir' argument is mandatory. " + "User must also provide an ingest method.", options, true);
      LOG.error("Error whilst parsing command line.", e);
    }
  }

  private static void loadFullConfig(MudrodEngine me, String dataDir) {
    //TODO all of the properties defined below, which are determined are
    //runtime need to be added to MudrodConstants.java and referenced 
    //accordingly and consistently from Properties.getProperty(MudrodConstant...);
    me.props.put("ontologyInputDir", dataDir + "SWEET_ocean/");
    me.props.put("oceanTriples", dataDir + "Ocean_triples.csv");
    me.props.put("userHistoryMatrix", dataDir + "UserHistoryMatrix.csv");
    me.props.put("clickstreamMatrix", dataDir + "ClickstreamMatrix.csv");
    me.props.put("metadataMatrix", dataDir + "MetadataMatrix.csv");
    me.props.put("clickstreamSVDMatrix_tmp", dataDir + "clickstreamSVDMatrix_tmp.csv");
    me.props.put("metadataSVDMatrix_tmp", dataDir + "metadataSVDMatrix_tmp.csv");
    me.props.put("raw_metadataPath", dataDir + me.props.getProperty(MudrodConstants.RAW_METADATA_TYPE));

    me.props.put("jtopia", dataDir + "jtopiaModel");
    me.props.put("metadata_term_tfidf_matrix", dataDir + "metadata_term_tfidf.csv");
    me.props.put("metadata_word_tfidf_matrix", dataDir + "metadata_word_tfidf.csv");
    me.props.put("session_metadata_Matrix", dataDir + "metadata_session_coocurrence_matrix.csv");

    me.props.put("metadataOBCode", dataDir + "MetadataOHCode");
    me.props.put("metadata_topic", dataDir + "metadata_topic");
    me.props.put("metadata_topic_matrix", dataDir + "metadata_topic_matrix.csv");
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
   * @param sparkDriver
   *          a configured {@link SparkDriver}
   */
  public void setSparkDriver(SparkDriver sparkDriver) {
    this.spark = sparkDriver;

  }
}
