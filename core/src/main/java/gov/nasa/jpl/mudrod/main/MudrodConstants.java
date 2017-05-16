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

import gov.nasa.jpl.mudrod.ontology.Ontology;

/**
 * Class contains static constant keys and values relating to Mudrod
 * configuration properties. Property values are read from <a href=
 * "https://github.com/mudrod/mudrod/blob/master/core/src/main/resources/config.xml">config.xml</a>
 */
public interface MudrodConstants {

  public static final String CLEANUP_TYPE_PREFIX = "Cleanup_type_prefix";

  public static final String CLICK_STREAM_LINKAGE_TYPE = "clickStreamLinkageType";

  public static final String CLICK_STREAM_MATRIX_TYPE = "clickStreamMatrixType";

  public static final String CLICKSTREAM_SVD_DIM = "clickstreamSVDDimension";

  public static final String CLICKSTREAM_W = "clickStream_w";

  public static final String COMMENT_TYPE = "commentType";

  /** Defined on CLI */
  public static final String DATA_DIR = "dataDir";

  public static final String DOWNLOAD_F = "downloadf";

  public static final String DOWNLOAD_WEIGHT = "downloadWeight";

  public static final String ES_CLUSTER = "clusterName";

  public static final String ES_TRANSPORT_TCP_PORT = "ES_Transport_TCP_Port";

  public static final String ES_UNICAST_HOSTS = "ES_unicast_hosts";

  public static final String ES_HTTP_PORT = "ES_HTTP_port";

  public static final String ES_INDEX_NAME = "indexName";

  public static final String FTP_PREFIX = "ftpPrefix";

  public static final String FTP_TYPE_PREFIX = "FTP_type_prefix";

  public static final String HTTP_PREFIX = "httpPrefix";

  public static final String HTTP_TYPE_PREFIX = "HTTP_type_prefix";

  public static final String LOG_INDEX = "logIndexName";

  public static final String METADATA_LINKAGE_TYPE = "metadataLinkageType";

  public static final String METADATA_SVD_DIM = "metadataSVDDimension";

  public static final String METADATA_URL = "metadataurl";

  public static final String METADATA_W = "metadata_w";

  public static final String MINI_USER_HISTORY = "mini_userHistory";

  public static final String MUDROD = "mudrod";

  /** Defined on CLI */
  public static final String MUDROD_CONFIG = "MUDROD_CONFIG";
  /**
   * An {@link Ontology} implementation.
   */
  public static final String ONTOLOGY_IMPL = MUDROD + "ontology.implementation";

  public static final String ONTOLOGY_LINKAGE_TYPE = "ontologyLinkageType";

  public static final String ONTOLOGY_W = "ontology_w";

  public static final String PROCESS_TYPE = "processingType";

  /** Defined on CLI */
  public static final String RAW_METADATA_PATH = "raw_metadataPath";

  public static final String RAW_METADATA_TYPE = "raw_metadataType";

  public static final String SEARCH_F = "searchf";

  public static final String SENDING_RATE = "sendingrate";

  public static final String SESSION_PORT = "SessionPort";

  public static final String SESSION_STATS_PREFIX = "SessionStats_prefix";

  public static final String SESSION_URL = "SessionUrl";

  public static final String SPARK_APP_NAME = "spark.app.name";

  public static final String SPARK_MASTER = "spark.master";
  /**
   * Absolute local location of javaSVMWithSGDModel directory. This is typically
   * <code>file:///usr/local/mudrod/core/src/main/resources/javaSVMWithSGDModel</code>
   */
  public static final String SVM_SGD_MODEL = "svmSgdModel";

  public static final String TIMEGAP = "timegap";

  public static final String TIME_SUFFIX = "TimeSuffix";

  public static final String USE_HISTORY_LINKAGE_TYPE = "userHistoryLinkageType";

  public static final String USER_HISTORY_W = "userHistory_w";

  public static final String VIEW_F = "viewf";

}
