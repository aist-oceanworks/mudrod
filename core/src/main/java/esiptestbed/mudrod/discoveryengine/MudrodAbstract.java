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

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.main.MudrodConstants;

/**
 * This is the most generic class of Mudrod
 */
public abstract class MudrodAbstract implements Serializable {

  private static final Logger LOG = LoggerFactory
      .getLogger(MudrodAbstract.class);
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private static final String TIME_SUFFIX = "TimeSuffix";
  protected Properties props = new Properties();
  protected ESDriver es = null;
  protected SparkDriver spark = null;
  protected long startTime;
  protected long endTime;
  public String httpType = null;
  public String ftpType = null;
  public String cleanupType = null;
  public String sessionStats = null;

  private static final String ES_SETTINGS = "elastic_settings.json";
  private static final String ES_MAPPINGS = "elastic_mappings.json";
  private int printLevel = 0;

  public MudrodAbstract(Properties props, ESDriver es, SparkDriver spark) {
    this.props = props;
    this.es = es;
    this.spark = spark;

    if (this.props != null) {
      this.initMudrod();
    }
  }

  protected void initMudrod() {
    InputStream settingsStream = getClass().getClassLoader()
        .getResourceAsStream(ES_SETTINGS);
    InputStream mappingsStream = getClass().getClassLoader()
        .getResourceAsStream(ES_MAPPINGS);
    JSONObject settingsJSON = null;
    JSONObject mappingJSON = null;

    try {
      settingsJSON = new JSONObject(
          IOUtils.toString(settingsStream, Charset.defaultCharset()));
    } catch (JSONException | IOException e1) {
      e1.printStackTrace();
    }

    try {
      mappingJSON = new JSONObject(
          IOUtils.toString(mappingsStream, Charset.defaultCharset()));
    } catch (JSONException | IOException e1) {
      e1.printStackTrace();
    }

    try {
      this.es.putMapping(props.getProperty(MudrodConstants.ES_INDEX_NAME),
          settingsJSON.toString(), mappingJSON.toString());
    } catch (IOException e) {
      LOG.error("Error entering Elasticsearch Mappings!", e);
    }

    httpType = props.getProperty("HTTP_type_prefix")
        + props.getProperty(TIME_SUFFIX);
    ftpType = props.getProperty("FTP_type_prefix")
        + props.getProperty(TIME_SUFFIX);
    cleanupType = props.getProperty("Cleanup_type_prefix")
        + props.getProperty(TIME_SUFFIX);
    sessionStats = props.getProperty("SessionStats_prefix")
        + props.getProperty(TIME_SUFFIX);

    printLevel = Integer.parseInt(props.getProperty("loglevel"));
  }

  public void print(String log, int level) {
    if (level <= printLevel) {
      LOG.info(log);
    }
  }

  public ESDriver getES() {
    return this.es;
  }

  public Properties getConfig() {
    return this.props;
  }
}
