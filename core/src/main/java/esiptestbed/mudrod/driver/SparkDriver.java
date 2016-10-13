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

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
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
  public SQLContext sqlContext;

  public SparkDriver() {
    SparkConf conf = new SparkConf().setAppName("Testing").setMaster("local[2]")
        .set("spark.hadoop.validateOutputSpecs", "false")
        .set("spark.files.overwrite", "true");
    sc = new JavaSparkContext(conf);
    sqlContext = new SQLContext(sc);
  }

  public void close()
  {
    sc.sc().stop();
  }
}
