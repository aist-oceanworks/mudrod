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
package esiptestbed.mudrod.weblog.process;

import java.util.List;
import java.util.Map;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.semantics.SemanticAnalyzer;
import esiptestbed.mudrod.utils.LinkageTriple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserHistoryAnalyzer extends DiscoveryStepAbstract {
  
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(UserHistoryAnalyzer.class);
  
  public UserHistoryAnalyzer(Map<String, String> config, ESDriver es,
      SparkDriver spark) {
    super(config, es, spark);
  }

  @Override
  public Object execute() {
    LOG.info("*****************UserHistoryAnalyzer starts******************");
    startTime = System.currentTimeMillis();

    SemanticAnalyzer sa = new SemanticAnalyzer(config, es, spark);
    List<LinkageTriple> tripleList = sa
        .CalTermSimfromMatrix(config.get("userHistoryMatrix"));
    sa.SaveToES(tripleList, config.get("indexName"),
        config.get("userHistoryLinkageType"));

    endTime = System.currentTimeMillis();
    es.refreshIndex();
    LOG.info("*****************UserHistoryAnalyzer ends******************Took {}s"
        , (endTime - startTime) / 1000);
    return null;
  }

  @Override
  public Object execute(Object o) {
    return null;
  }

}
