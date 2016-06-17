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
package esiptestbed.mudrod.metadata.structure;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.codehaus.jettison.json.JSONObject;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import esiptestbed.mudrod.driver.ESDriver;
import scala.Tuple2;

public class MetadataExtractor implements Serializable {

  public MetadataExtractor() {
    // TODO Auto-generated constructor stub
  }

  public JavaPairRDD<String, List<String>> loadMetadata(ESDriver es,
      JavaSparkContext sc, String index, String type) throws Exception {
    List<PODAACMetadata> metadatas = this.loadMetadataFromES(es, index, type);
    JavaPairRDD<String, List<String>> metadataTermsRDD = this
        .buildMetadataRDD(es, sc, index, metadatas);
    return metadataTermsRDD;
  }

  protected List<PODAACMetadata> loadMetadataFromES(ESDriver es, String index,
      String type) throws Exception {

    List<PODAACMetadata> metadatas = new ArrayList<PODAACMetadata>();
    SearchResponse scrollResp = es.client.prepareSearch(index).setTypes(type)
        .setQuery(QueryBuilders.matchAllQuery()).setScroll(new TimeValue(60000))
        .setSize(100).execute().actionGet();

    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> result = hit.getSource();
        String shortname = (String) result.get("Dataset-ShortName");
        List<String> topic = (List<String>) result
            .get("DatasetParameter-Topic");
        List<String> term = (List<String>) result.get("DatasetParameter-Term");
        List<String> keyword = (List<String>) result.get("Dataset-Metadata");
        List<String> variable = (List<String>) result
            .get("DatasetParameter-Variable");
        List<String> longname = (List<String>) result
            .get("DatasetProject-Project-LongName");
        PODAACMetadata metadata = new PODAACMetadata(shortname, longname,
            es.customAnalyzing(index, topic), es.customAnalyzing(index, term),
            es.customAnalyzing(index, variable),
            es.customAnalyzing(index, keyword));
        metadatas.add(metadata);

        /*
         * Please look at these fields! "Dataset-Metadata", "Dataset-ShortName",
         * "Dataset-LongName", "Dataset-Description", "DatasetParameter-*",
         * "DatasetSource-*"
         */
      }
      scrollResp = es.client.prepareSearchScroll(scrollResp.getScrollId())
          .setScroll(new TimeValue(600000)).execute().actionGet();
      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }
    }

    return metadatas;
  }

  protected JavaPairRDD<String, List<String>> buildMetadataRDD(ESDriver es,
      JavaSparkContext sc, String index, List<PODAACMetadata> metadatas) {
    JavaRDD<PODAACMetadata> metadataRDD = sc.parallelize(metadatas);
    JavaPairRDD<String, List<String>> metadataTermsRDD = metadataRDD
        .mapToPair(new PairFunction<PODAACMetadata, String, List<String>>() {
          public Tuple2<String, List<String>> call(PODAACMetadata metadata)
              throws Exception {
            return new Tuple2<String, List<String>>(metadata.getShortName(),
                metadata.getAllTermList());
          }
        })
        .reduceByKey(new Function2<List<String>, List<String>, List<String>>() {
          public List<String> call(List<String> v1, List<String> v2)
              throws Exception {
            // TODO Auto-generated method stub
            List<String> list = new ArrayList<String>();
            list.addAll(v1);
            list.addAll(v2);
            return list;
          }
        });

    return metadataTermsRDD;
  }
}
