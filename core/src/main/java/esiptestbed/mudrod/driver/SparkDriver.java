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

public class SparkDriver implements Serializable {
  /**
   *
   */
  private static final long serialVersionUID = 1L;
  public transient JavaSparkContext sc;
  public transient SQLContext sqlContext;

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
