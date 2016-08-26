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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import esiptestbed.mudrod.driver.ESDriver;

public class OHCodeExtractor implements Serializable {

  private String indexName;
  private String metadataType;

  public OHCodeExtractor(Properties props) {
    // TODO Auto-generated constructor stub
    indexName = props.getProperty("indexName");
    metadataType = props.getProperty("recom_metadataType");
  }

  public List<String> loadMetadataOHEncode(ESDriver es) throws Exception {

    OHEncoder coder = new OHEncoder();
    List<String> fields = coder.CategoricalVars;
    // fields.add("DatasetParameter-Term");
    List<String> metadataCode = this.loadFieldsOHEncode(es, fields);

    return metadataCode;
  }

  public List<String> loadFieldsOHEncode(ESDriver es, List<String> fields) {

    List<String> metedataCode = new ArrayList<String>();
    SearchResponse scrollResp = es.getClient().prepareSearch(indexName)
        .setTypes(metadataType).setScroll(new TimeValue(60000))
        .setQuery(QueryBuilders.matchAllQuery()).setSize(100).execute()
        .actionGet();

    int fieldnum = fields.size();
    OHEncoder coder = new OHEncoder();
    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> metadata = hit.getSource();
        Map<String, Object> metadatacode;

        String shortname = (String) metadata.get("Dataset-ShortName");

        String[] codeArr = new String[fieldnum];
        for (int i = 0; i < fieldnum; i++) {
          String field = fields.get(i);
          String code = (String) metadata.get(field + "_code");
          codeArr[i] = code;
        }
        String codeStr = String.join(" & ", codeArr);

        metedataCode.add(shortname + ":" + codeStr);
      }

      scrollResp = es.getClient().prepareSearchScroll(scrollResp.getScrollId())
          .setScroll(new TimeValue(600000)).execute().actionGet();
      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }
    }

    return metedataCode;
  }

  public Map<String, Vector> loadFieldsOHEncodeMap(ESDriver es,
      List<String> fields) {

    Map<String, Vector> metedataCode = new HashMap<String, Vector>();
    SearchResponse scrollResp = es.getClient().prepareSearch(indexName)
        .setTypes(metadataType).setScroll(new TimeValue(60000))
        .setQuery(QueryBuilders.matchAllQuery()).setSize(100).execute()
        .actionGet();

    int fieldnum = fields.size();
    OHEncoder coder = new OHEncoder();
    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> metadata = hit.getSource();
        Map<String, Object> metadatacode;

        String shortname = (String) metadata.get("Dataset-ShortName");

        double[] codeArr = null;
        for (int i = 0; i < fieldnum; i++) {
          String field = fields.get(i);
          String code = (String) metadata.get(field + "_code");
          String[] values = code.split(",");
          int valuesize = values.length;
          double[] nums = Stream.of(values).mapToDouble(Double::parseDouble)
              .toArray();

          // add weight
          int arrLen = nums.length;
          for (int k = 0; k < arrLen; k++) {
            nums[k] = nums[k] * coder.CategoricalVarWeights.get(field);
          }

          codeArr = ArrayUtils.addAll(nums, codeArr);
        }
        Vector vec = Vectors.dense(codeArr);

        metedataCode.put(shortname.toLowerCase(), vec);
      }

      scrollResp = es.getClient().prepareSearchScroll(scrollResp.getScrollId())
          .setScroll(new TimeValue(600000)).execute().actionGet();
      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }
    }

    return metedataCode;
  }

  /*public JavaPairRDD<String, Vector> loadOBCode(ESDriver es, JavaSparkContext sc, String index, String type)
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

  protected JavaPairRDD<String, Vector> buildOBCodeRDD(ESDriver es, JavaSparkContext sc, String index,
  		List<String> metadatacodes) {
  	JavaRDD<String> metadataCodeRDD = sc.parallelize(metadatacodes);
  	JavaPairRDD<String, Vector> codeVecRDD = metadataCodeRDD.mapToPair(new PairFunction<String, String, Vector>() {
  		public Tuple2<String, Vector> call(String metadatacide) throws Exception {

  			String[] tmps = metadatacide.split(":");
  			String[] values = tmps[1].split(",");

  			double[] nums = new double[values.length];
  			for (int i = 0; i < nums.length; i++) {
  				nums[i] = Double.parseDouble(values[i]);
  			}

  			Vector vec = Vectors.dense(nums);

  			return new Tuple2<String, Vector>(tmps[0].toLowerCase(), vec);
  		}
  	});

  	return codeVecRDD;
  }*/
}
