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
package esiptestbed.mudrod.metadata.process;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.metadata.structure.MetadataExtractor;
import esiptestbed.mudrod.semantics.SVDAnalyzer;
import esiptestbed.mudrod.utils.LinkageTriple;
import esiptestbed.mudrod.utils.SVDUtil;

public class MetadataAnalyzer extends DiscoveryStepAbstract
    implements Serializable {
  public MetadataAnalyzer(Map<String, String> config, ESDriver es,
      SparkDriver spark) {
    super(config, es, spark);
    // TODO Auto-generated constructor stub
  }

  @Override
  public Object execute(Object o) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Object execute() {
    // TODO Auto-generated method stub
    try {
      System.out.println(
          "*****************Metadata Analyzer starts******************");
      startTime = System.currentTimeMillis();

      SVDAnalyzer analyzer = new SVDAnalyzer(config, es, spark);
      int svdDimension = Integer.parseInt(config.get("metadataSVDDimension"));
      String metadata_matrix_file = config.get("metadataMatrix");
      String svd_matrix_fileName = config.get("metadataSVDMatrix_tmp");

      analyzer.GetSVDMatrix(metadata_matrix_file, svdDimension,
          svd_matrix_fileName);
      List<LinkageTriple> triples = analyzer
          .CalTermSimfromMatrix(svd_matrix_fileName);

      analyzer.SaveToES(triples, config.get("indexName"),
          config.get("metadataLinkageType"));

    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    endTime = System.currentTimeMillis();
    es.refreshIndex();
    System.out.println(
        "*****************Metadata Analyzer ends******************Took "
            + (endTime - startTime) / 1000 + "s");
    return null;
  }
}
