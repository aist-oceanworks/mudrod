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
package gov.nasa.jpl.mudrod.driver;

import gov.nasa.jpl.mudrod.main.MudrodConstants;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.SQLContext;

import java.io.File;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.Properties;
//import org.apache.spark.sql.SparkSession;

public class SparkDriver implements Serializable {

  //TODO the commented out code below is the API uprgade
  //for Spark 2.0.0. It requires a large upgrade and simplification
  //across the mudrod codebase so should be done in an individual ticket.
  //  /**
  //   *
  //   */
  //  private static final long serialVersionUID = 1L;
  //  private SparkSession builder;
  //
  //  public SparkDriver() {
  //    builder = SparkSession.builder()
  //        .master("local[2]")
  //        .config("spark.hadoop.validateOutputSpecs", "false")
  //        .config("spark.files.overwrite", "true")
  //        .getOrCreate();
  //  }
  //
  //  public SparkSession getBuilder() {
  //    return builder;
  //  }
  //
  //  public void setBuilder(SparkSession builder) {
  //    this.builder = builder;
  //  }
  //
  //  public void close() {
  //    builder.stop();
  //  }

  /**
   *
   */
  private static final long serialVersionUID = 1L;
  public transient JavaSparkContext sc;
  public transient SQLContext sqlContext;

  public SparkDriver() {
    // empty default constructor
  }

  public SparkDriver(Properties props) {
    SparkConf conf = new SparkConf().setAppName(props.getProperty(MudrodConstants.SPARK_APP_NAME, "MudrodSparkApp")).setIfMissing("spark.master", props.getProperty(MudrodConstants.SPARK_MASTER))
        .set("spark.hadoop.validateOutputSpecs", "false").set("spark.files.overwrite", "true");

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
