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

/**
 * Class contains static constant keys and values relating to Mudrod
 * configuration properties. Property values are read from <a href=
 * "https://github.com/mudrod/mudrod/blob/master/core/src/main/resources/config.xml">config.xml</a>
 */
public interface MudrodConstants {

  public static final String CLEANUP_TYPE = "cleanupLog";

  public static final String CLICK_STREAM_LINKAGE_TYPE = "clickStreamLinkage";

  public static final String CLICK_STREAM_MATRIX_TYPE = "clickStreamMatrix";

  public static final String CLICKSTREAM_SVD_DIM = "mudrod.clickstream.svd.d";

  public static final String CLICKSTREAM_W = "mudrod.clickstream.weight";
  
  public static final String CLICKSTREAM_PATH = "mudrod.clickstream.path";
  
  public static final String CLICKSTREAM_SVD_PATH = "mudrod.clickstream.svd.path";

  /** Defined on CLI */
  public static final String DATA_DIR = "dataDir";

  public static final String DOWNLOAD_WEIGHT = "mudrod.download.weight";

  public static final String ES_CLUSTER = "mudrod.cluster.name";

  public static final String ES_TRANSPORT_TCP_PORT = "mudrod.es.transport.tcp.port";

  public static final String ES_UNICAST_HOSTS = "mudrod.es.unicast.hosts";

  public static final String ES_HTTP_PORT = "mudrod.es.http.port";

  public static final String ES_INDEX_NAME = "mudrod.es.index";

  public static final String FTP_PREFIX = "mudrod.ftp.prefix";

  public static final String FTP_TYPE = "rawftp";
  
  public static final String FTP_LOG = "ftp";

  public static final String HTTP_PREFIX = "mudrod.http.prefix";

  public static final String HTTP_TYPE = "rawhttp";
  
  public static final String HTTP_LOG = "http";
  
  public static final String BASE_URL = "mudrod.base.url";
  
  public static final String BLACK_LIST_REQUEST = "mudrod.black.request.list";
  
  public static final String BLACK_LIST_AGENT = "mudrod.black.agent.list";

  public static final String LOG_INDEX = "mudrod.log.index";

  public static final String METADATA_LINKAGE_TYPE = "MetadataLinkage";
  
  public static final String METADATA_DOWNLOAD_URL = "mudrod.metadata.download.url";

  public static final String METADATA_SVD_DIM = "mudrod.metadata.svd.d";

  public static final String METADATA_URL = "mudrod.metadata.url";

  public static final String METADATA_W = "mudrod.metadata.weight";

  public static final String QUERY_MIN = "mudrod.query.min";

  public static final String MUDROD = "mudrod";

  /** Defined on CLI */
  public static final String MUDROD_CONFIG = "MUDROD_CONFIG";
  /**
   * An {@link Ontology} implementation.
   */
  public static final String ONTOLOGY_IMPL = MUDROD + "ontology.implementation";

  public static final String ONTOLOGY_LINKAGE_TYPE = "ontologyLinkage";

  public static final String ONTOLOGY_W = "mudrod.ontology.weight";
  
  public static final String ONTOLOGY_PATH = "mudrod.ontology.path";
  
  public static final String ONTOLOGY_INPUT_PATH = "mudrod.ontology.input.path";

  public static final String PROCESS_TYPE = "mudrod.processing.type";

  /** Defined on CLI */
  public static final String METADATA_DOWNLOAD = "mudrod.metadata.download";
  
  public static final String RAW_METADATA_PATH = "mudrod.metadata.path";

  public static final String RAW_METADATA_TYPE = "mudrod.metadata.type";
  
  public static final String METADATA_MATRIX_PATH = "mudrod.metadata.matrix.path";
  
  public static final String METADATA_SVD_PATH = "mudrod.metadata.svd.path";
  
  public static final String RECOM_METADATA_TYPE = "recommedation.metadata";
  
  public static final String METADATA_ID = "mudrod.metadata.id";
  
  public static final String SEMANTIC_FIELDS = "mudrod.metadata.semantic.fields";
  
  public static final String METADATA_WORD_SIM_TYPE = "metadata.word.sim";
  
  public static final String METADATA_FEATURE_SIM_TYPE = "metadata.feature.sim";
  
  public static final String METADATA_SESSION_SIM_TYPE = "metadata.session.sim";
  
  public static final String METADATA_TERM_MATRIX_PATH = "metadata.term.matrix.path";
  
  public static final String METADATA_WORD_MATRIX_PATH = "metadata.word.matrix.path";
  
  public static final String METADATA_SESSION_MATRIX_PATH = "metadata.session.matrix.path";

  public static final String REQUEST_RATE = "mudrod.request.rate";

  public static final String SESSION_PORT = "mudrod.session.port";

  public static final String SESSION_STATS_TYPE = "sessionstats";

  public static final String SESSION_URL = "mudrod.session.url";

  public static final String SPARK_APP_NAME = "mudrod.spark.app.name";

  public static final String SPARK_MASTER = "mudrod.spark.master";
  /**
   * Absolute local location of javaSVMWithSGDModel directory. This is typically
   * <code>file:///usr/local/mudrod/core/src/main/resources/javaSVMWithSGDModel</code>
   */
  public static final String RANKING_MODEL = "mudrod.ranking.model";

  public static final String REQUEST_TIME_GAP = "mudrod.request.time.gap";

  public static final String TIME_SUFFIX = "TimeSuffix";

  public static final String USE_HISTORY_LINKAGE_TYPE = "userHistoryLinkage";

  public static final String USER_HISTORY_W = "mudrod.user.history.weight";
  
  public static final String USER_HISTORY_PATH = "mudrod.user.history.path";

  public static final String VIEW_F = "mudrod.view.freq";
  
  public static final String VIEW_MARKER = "mudrod.view.url.marker";
  
  public static final String SEARCH_MARKER = "mudrod.search.url.marker";
  
  public static final String SEARCH_F = "mudrod.search.freq";
  
  public static final String DOWNLOAD_F = "mudrod.download.freq";

}
