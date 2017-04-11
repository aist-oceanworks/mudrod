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
package gov.nasa.jpl.mudrod.metadata.structure;

import gov.nasa.jpl.mudrod.driver.ESDriver;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class MetadataExtractor implements Serializable {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  public MetadataExtractor() {
  }

  /**
   * loadMetadata:Load all metadata from Elasticsearch and convert them to
   * pairRDD Please make sure metadata has been already harvested from web
   * service and stored in Elasticsearch.
   *
   * @param es    an Elasticsearch client node instance
   * @param sc    spark context
   * @param index index name of log processing application
   * @param type  metadata type name
   * @return PairRDD, in each pair key is metadata short name and value is term
   * list extracted from metadata variables.
   */
  public JavaPairRDD<String, List<String>> loadMetadata(ESDriver es, JavaSparkContext sc, String index, String type) {
    List<PODAACMetadata> metadatas = this.loadMetadataFromES(es, index, type);
    JavaPairRDD<String, List<String>> metadataTermsRDD = this.buildMetadataRDD(es, sc, index, metadatas);
    return metadataTermsRDD;
  }

  /**
   * loadMetadataFromES: Load all metadata from Elasticsearch.
   *
   * @param es    an Elasticsearch client node instance
   * @param index index name of log processing application
   * @param type  metadata type name
   * @return metadata list
   */
  protected List<PODAACMetadata> loadMetadataFromES(ESDriver es, String index, String type) {

    List<PODAACMetadata> metadatas = new ArrayList<PODAACMetadata>();
    SearchResponse scrollResp = es.getClient().prepareSearch(index).setTypes(type).setQuery(QueryBuilders.matchAllQuery()).setScroll(new TimeValue(60000)).setSize(100).execute().actionGet();

    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> result = hit.getSource();
        String shortname = (String) result.get("Dataset-ShortName");
        List<String> topic = (List<String>) result.get("DatasetParameter-Topic");
        List<String> term = (List<String>) result.get("DatasetParameter-Term");
        List<String> keyword = (List<String>) result.get("Dataset-Metadata");
        List<String> variable = (List<String>) result.get("DatasetParameter-Variable");
        List<String> longname = (List<String>) result.get("DatasetProject-Project-LongName");

        List<String> region = (List<String>) result.get("DatasetRegion-Region");

        PODAACMetadata metadata = null;
        try {
          metadata = new PODAACMetadata(shortname, longname, es.customAnalyzing(index, topic), es.customAnalyzing(index, term), es.customAnalyzing(index, variable), es.customAnalyzing(index, keyword),
              es.customAnalyzing(index, region));
        } catch (InterruptedException | ExecutionException e) {
          e.printStackTrace();

        }
        metadatas.add(metadata);
      }
      scrollResp = es.getClient().prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }
    }

    return metadatas;
  }

  /**
   * buildMetadataRDD: Convert metadata list to JavaPairRDD
   *
   * @param es        an Elasticsearch client node instance
   * @param sc        spark context
   * @param index     index name of log processing application
   * @param metadatas metadata list
   * @return PairRDD, in each pair key is metadata short name and value is term
   * list extracted from metadata variables.
   */
  protected JavaPairRDD<String, List<String>> buildMetadataRDD(ESDriver es, JavaSparkContext sc, String index, List<PODAACMetadata> metadatas) {
    JavaRDD<PODAACMetadata> metadataRDD = sc.parallelize(metadatas);
    JavaPairRDD<String, List<String>> metadataTermsRDD = metadataRDD.mapToPair(new PairFunction<PODAACMetadata, String, List<String>>() {
      /**
       *
       */
      private static final long serialVersionUID = 1L;

      @Override
      public Tuple2<String, List<String>> call(PODAACMetadata metadata) throws Exception {
        return new Tuple2<String, List<String>>(metadata.getShortName(), metadata.getAllTermList());
      }
    }).reduceByKey(new Function2<List<String>, List<String>, List<String>>() {
      /**
       *
       */
      private static final long serialVersionUID = 1L;

      @Override
      public List<String> call(List<String> v1, List<String> v2) throws Exception {
        List<String> list = new ArrayList<String>();
        list.addAll(v1);
        list.addAll(v2);
        return list;
      }
    });

    return metadataTermsRDD;
  }
}
