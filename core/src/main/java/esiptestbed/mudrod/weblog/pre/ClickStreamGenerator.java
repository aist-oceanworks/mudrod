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
package esiptestbed.mudrod.weblog.pre;

import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.utils.MatrixUtil;
import esiptestbed.mudrod.utils.RDDUtil;
import esiptestbed.mudrod.weblog.structure.ClickStream;
import esiptestbed.mudrod.weblog.structure.SessionExtractor;

public class ClickStreamGenerator extends DiscoveryStepAbstract {

  public ClickStreamGenerator(Map<String, String> config, ESDriver es,
      SparkDriver spark) {
    super(config, es, spark);
    // TODO Auto-generated constructor stub
  }

  @Override
  public Object execute() {
    // TODO Auto-generated method stub
    System.out.println(
        "*****************ClickStreamGenerator starts******************");
    startTime = System.currentTimeMillis();

    String clickstrem_matrix_file = config.get("clickstreamMatrix");
    try {
      SessionExtractor extractor = new SessionExtractor();
      JavaRDD<ClickStream> clickstreamRDD = extractor
          .extractClickStreamFromES(this.config, this.es, this.spark);
      int weight = Integer.parseInt(config.get("downloadWeight"));
      JavaPairRDD<String, List<String>> metaddataQueryRDD = extractor
          .bulidDataQueryRDD(clickstreamRDD, weight);
      RowMatrix wordDocMatrix = MatrixUtil
          .createWordDocMatrix(metaddataQueryRDD, spark.sc);

      List<String> rowKeys = RDDUtil.getAllWordsInDoc(metaddataQueryRDD)
          .collect();
      List<String> colKeys = metaddataQueryRDD.keys().collect();
      MatrixUtil.exportToCSV(wordDocMatrix, rowKeys, colKeys,
          clickstrem_matrix_file);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    endTime = System.currentTimeMillis();
    System.out.println(
        "*****************ClickStreamGenerator ends******************Took "
            + (endTime - startTime) / 1000 + "s");
    return null;
  }

  @Override
  public Object execute(Object o) {
    // TODO Auto-generated method stub
    return null;
  }

}
