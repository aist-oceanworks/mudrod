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
import org.apache.commons.io.IOUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckForNull;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

/**
 * This is the most generic class of Mudrod
 */
public abstract class MudrodAbstract implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(MudrodAbstract.class);
  /**
   *
   */
  private static final long serialVersionUID = 1L;
  protected Properties props = new Properties();
  protected ESDriver es = null;
  protected SparkDriver spark = null;
  protected long startTime;
  protected long endTime;

  protected static final String ES_SETTINGS = "elastic_settings.json";
  protected static final String ES_MAPPINGS = "elastic_mappings.json";

  public MudrodAbstract(Properties props, ESDriver es, SparkDriver spark) {
    this.props = props;
    this.es = es;
    this.spark = spark;

    if (this.props != null) {
      this.initMudrod();
    }
  }

  /**
   * Method of setting up essential configuration for MUDROD to start
   */
  @CheckForNull
  protected void initMudrod() {
    InputStream settingsStream = getClass().getClassLoader().getResourceAsStream(ES_SETTINGS);
    InputStream mappingsStream = getClass().getClassLoader().getResourceAsStream(ES_MAPPINGS);
    JSONObject settingsJSON = null;
    JSONObject mappingJSON = null;

    try {
      settingsJSON = new JSONObject(IOUtils.toString(settingsStream));
    } catch (JSONException | IOException e1) {
      LOG.error("Error reading Elasticsearch settings!", e1);
    }

    try {
      mappingJSON = new JSONObject(IOUtils.toString(mappingsStream));
    } catch (JSONException | IOException e1) {
      LOG.error("Error reading Elasticsearch mappings!", e1);
    }

    try {
      if (settingsJSON != null && mappingJSON != null) {
        this.es.putMapping(props.getProperty(MudrodConstants.ES_INDEX_NAME), settingsJSON.toString(), mappingJSON.toString());
      }
    } catch (IOException e) {
      LOG.error("Error entering Elasticsearch Mappings!", e);
    }
  }

  /**
   * Get driver of Elasticsearch
   *
   * @return driver of Elasticsearch
   */
  public ESDriver getES() {
    return this.es;
  }

  /**
   * Get configuration of MUDROD (read from configuration file)
   *
   * @return configuration of MUDROD
   */
  public Properties getConfig() {
    return this.props;
  }
}
