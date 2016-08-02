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
package esiptestbed.mudrod.recommendation.pre;

import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.utils.LabeledRowMatrix;
import esiptestbed.mudrod.utils.MatrixUtil;
import esiptestbed.mudrod.weblog.structure.ClickStream;
import esiptestbed.mudrod.weblog.structure.SessionExtractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ItemRateGenerator extends DiscoveryStepAbstract {
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(ItemRateGenerator.class);

  public ItemRateGenerator(Map<String, String> config, ESDriver es,
      SparkDriver spark) {
    super(config, es, spark);
  }

  @Override
  public Object execute() {
    LOG.info("*****************ItemRateGenerator starts******************");
    startTime = System.currentTimeMillis();

    try {
      SessionExtractor extractor = new SessionExtractor();
      JavaRDD<ClickStream> clickstreamRDD = extractor
          .extractClickStreamFromES(this.config, this.es, this.spark);
      JavaPairRDD<String, Double> useritem_rateRDD = extractor.bulidUserItermRDD(clickstreamRDD);
      useritem_rateRDD/*.coalesce(1,true)*/.saveAsTextFile(config.get("user_item_rate"));
      
    } catch (Exception e) {
      e.printStackTrace();
    }

    endTime = System.currentTimeMillis();
    LOG.info("*****************ItemRateGenerator ends******************Took {}s",
        (endTime - startTime) / 1000);
    return null;
  }

  @Override
  public Object execute(Object o) {
    return null;
  }

}
