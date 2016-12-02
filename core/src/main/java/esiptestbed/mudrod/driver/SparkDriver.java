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
package esiptestbed.mudrod.driver;

import java.io.Serializable;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.SQLContext;

import esiptestbed.mudrod.main.MudrodConstants;

public class SparkDriver implements Serializable {
  /**
   *
   */
  private static final long serialVersionUID = 1L;
  public transient JavaSparkContext sc;
  public SQLContext sqlContext;

  public SparkDriver() {
    // empty default constructor
  }

  public SparkDriver(Properties props) {
    SparkConf conf = new SparkConf()
        .setAppName(
            props.getProperty(MudrodConstants.SPARK_APP_NAME, "MudrodSparkApp"))
        .setIfMissing("spark.master",
            props.getProperty(MudrodConstants.SPARK_MASTER))
        .set("spark.hadoop.validateOutputSpecs", "false")
        .set("spark.files.overwrite", "true");

    String esHost = props.getProperty(MudrodConstants.ES_UNICAST_HOSTS);
    String esPort = props.getProperty(MudrodConstants.ES_HTTP_PORT);

    if (!"".equals(esHost)) {
      conf.set("es.nodes", esHost);
    }

    if (!"".equals(esPort)) {
      conf.set("es.port", esPort);
    }

    conf.set("spark.serializer", KryoSerializer.class.getName());
    conf.set("es.batch.size.entries", "1500");

    sc = new JavaSparkContext(conf);
    sqlContext = new SQLContext(sc);
  }

  public void close() {
    sc.sc().stop();
  }
}
