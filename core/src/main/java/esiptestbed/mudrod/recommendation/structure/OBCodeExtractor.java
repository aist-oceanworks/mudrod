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
package esiptestbed.mudrod.recommendation.structure;

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
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.metadata.structure.PODAACMetadata;
import scala.Tuple2;

public class OBCodeExtractor implements Serializable {

  public OBCodeExtractor() {
    // TODO Auto-generated constructor stub
  }

	public JavaPairRDD<String, Vector> loadOBCode(ESDriver es, JavaSparkContext sc, String index, String type)
			throws Exception {

		List<String> metadataCodes = this.loadOBCodeFromES(es, index, type);
		JavaPairRDD<String, Vector> metadataCodeRDD = this.buildOBCodeRDD(es, sc, index, metadataCodes);
		return metadataCodeRDD;

	}

	private List<String> loadOBCodeFromES(ESDriver es, String index, String type) throws Exception {

		List<String> metadataCodes = new ArrayList<String>();

		SearchResponse scrollResp = es.client.prepareSearch(index).setTypes(type)
				.setQuery(QueryBuilders.matchAllQuery()).setScroll(new TimeValue(60000)).setSize(100).execute()
				.actionGet();

		while (true) {
			for (SearchHit hit : scrollResp.getHits().getHits()) {
				Map<String, Object> result = hit.getSource();
				String shortname = (String) result.get("Dataset-ShortName");
				String obCode = (String) result.get("Metadata_code");
				metadataCodes.add(shortname + ":" + obCode);
			}
			scrollResp = es.client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000))
					.execute().actionGet();
			if (scrollResp.getHits().getHits().length == 0) {
				break;
			}
		}

		return metadataCodes;
	}
  

  protected JavaPairRDD<String, Vector> buildOBCodeRDD(ESDriver es,JavaSparkContext sc, String index, List<String> metadatacodes) {
    JavaRDD<String> metadataCodeRDD = sc.parallelize(metadatacodes);
    JavaPairRDD<String, Vector> codeVecRDD = metadataCodeRDD
        .mapToPair(new PairFunction<String, String, Vector>() {
          public Tuple2<String, Vector> call(String metadatacide)
              throws Exception {
        	
        	String[] tmps = metadatacide.split(":");
        	String[] values = tmps[1].split(",");
        	
        	double[] nums = new double[values.length];
        	for (int i = 0; i < nums.length; i++) {
        	    nums[i] = Double.parseDouble(values[i]);
        	}
        	
        	Vector vec = Vectors.dense(nums);
        	
            return new Tuple2<String, Vector>(tmps[0],vec);
          }
        });

    return codeVecRDD;
  }
}
