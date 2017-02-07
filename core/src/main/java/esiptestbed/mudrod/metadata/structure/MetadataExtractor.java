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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import esiptestbed.mudrod.driver.ESDriver;
import scala.Tuple2;

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
   * @param es
   *          an Elasticsearch client node instance
   * @param sc
   *          spark context
   * @param index
   *          index name of log processing application
   * @param type
   *          metadata type name
   * @return PairRDD, in each pair key is metadata short name and value is term
   *         list extracted from metadata variables.
   */
  public JavaPairRDD<String, List<String>> loadMetadata(ESDriver es,
      JavaSparkContext sc, String index, String type) {
    List<Metadata> metadatas = this.loadMetadataFromES(es, index, type);
    JavaPairRDD<String, List<String>> metadataTermsRDD = this
        .buildMetadataRDD(es, sc, index, metadatas);
    return metadataTermsRDD;
  }

  /**
   * loadMetadataFromES: Load all metadata from Elasticsearch.
   *
   * @param es
   *          an Elasticsearch client node instance
   * @param index
   *          index name of log processing application
   * @param type
   *          metadata type name
   * @return metadata list
   */
  @SuppressWarnings("unchecked")
protected List<Metadata> loadMetadataFromES(ESDriver es, String index,
      String type) {

    List<Metadata> metadatas = new ArrayList<Metadata>();
    SearchResponse scrollResp = es.getClient().prepareSearch(index).setTypes(type)
        .setQuery(QueryBuilders.matchAllQuery()).setScroll(new TimeValue(60000))
        .setSize(100).execute().actionGet();

    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> result = hit.getSource();
        String shortname = (String) result.get("Dataset-ShortName");
        
        List<String> allterms = new ArrayList<String>();
        for(int m =0; m <Metadata.fieldsList.length; m++) {
        	List<String> list = (List<String>) result
                    .get(Metadata.fieldsList[m]);
        	if(list!=null && list.size()>0)
				try {
					allterms.addAll(es.customAnalyzing(index, list));
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
        }
        
        Metadata metadata = null;
        metadata = new Metadata(shortname);
        metadata.setAllterms(allterms);

        metadatas.add(metadata);
      }
      scrollResp = es.getClient().prepareSearchScroll(scrollResp.getScrollId())
          .setScroll(new TimeValue(600000)).execute().actionGet();
      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }
    }

    return metadatas;
  }

  /**
   * buildMetadataRDD: Convert metadata list to JavaPairRDD
   *
   * @param es
   *          an Elasticsearch client node instance
   * @param sc
   *          spark context
   * @param index
   *          index name of log processing application
   * @param metadatas
   *          metadata list
   * @return PairRDD, in each pair key is metadata short name and value is term
   *         list extracted from metadata variables.
   */
  protected JavaPairRDD<String, List<String>> buildMetadataRDD(ESDriver es,
      JavaSparkContext sc, String index, List<Metadata> metadatas) {
    JavaRDD<Metadata> metadataRDD = sc.parallelize(metadatas);
    JavaPairRDD<String, List<String>> metadataTermsRDD = metadataRDD
        .mapToPair(new PairFunction<Metadata, String, List<String>>() {
          /**
           * 
           */
          private static final long serialVersionUID = 1L;

          @Override
          public Tuple2<String, List<String>> call(Metadata metadata)
              throws Exception {
            return new Tuple2<String, List<String>>(metadata.shortname,
                metadata.getAllterms());
          }
        })
        .reduceByKey(new Function2<List<String>, List<String>, List<String>>() {
          /**
           * 
           */
          private static final long serialVersionUID = 1L;

          @Override
          public List<String> call(List<String> v1, List<String> v2)
              throws Exception {
            List<String> list = new ArrayList<String>();
            list.addAll(v1);
            list.addAll(v2);
            return list;
          }
        });

    return metadataTermsRDD;
  }
}
