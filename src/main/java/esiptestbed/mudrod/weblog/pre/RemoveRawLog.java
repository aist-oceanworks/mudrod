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

import java.util.Map;

import org.elasticsearch.index.query.QueryBuilders;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;

public class RemoveRawLog extends DiscoveryStepAbstract {

  public RemoveRawLog(Map<String, String> config, ESDriver es,
      SparkDriver spark) {
    super(config, es, spark);
    // TODO Auto-generated constructor stub
  }

  @Override
  public Object execute() {
    // TODO Auto-generated method stub
    System.out
        .println("*****************Clean raw log starts******************");
    startTime = System.currentTimeMillis();
    es.deleteAllByQuery(config.get("indexName"), HTTP_type,
        QueryBuilders.matchAllQuery());
    es.deleteAllByQuery(config.get("indexName"), FTP_type,
        QueryBuilders.matchAllQuery());
    endTime = System.currentTimeMillis();
    es.refreshIndex();
    System.out
        .println("*****************Clean raw log ends******************Took "
            + (endTime - startTime) / 1000 + "s");
    return null;
  }

  @Override
  public Object execute(Object o) {
    // TODO Auto-generated method stub
    return null;
  }

}
