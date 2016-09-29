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

/**
 * Extractor metadata code
 */
public class OHCodeExtractor implements Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  // index name:
  private String indexName;
  // type name of metadata
  private String metadataType;

  /**
   * Creates a new instance of OHCodeExtractor.
   *
   * @param props
   *          the Mudrod configuration
   */
  public OHCodeExtractor(Properties props) {
    indexName = props.getProperty("indexName");
    metadataType = props.getProperty("recom_metadataType");
  }

  /**
   * Load metadata code from es
   *
   * @param es
   *          the Elasticsearch client
   * @return code value of variables
   */
  public List<String> loadMetadataOHEncode(ESDriver es) {
    OHEncoder coder = new OHEncoder();
    List<String> fields = coder.CategoricalVars;
    return this.loadFieldsOHEncode(es, fields);
  }

  /**
   * load code value of giving variables
   *
   * @param es
   *          the Elasticsearch client
   * @param fields
   *          variables list
   * @return code ist
   */
  public List<String> loadFieldsOHEncode(ESDriver es, List<String> fields) {

    List<String> metedataCode = new ArrayList<>();
    SearchResponse scrollResp = es.getClient().prepareSearch(indexName)
        .setTypes(metadataType).setScroll(new TimeValue(60000))
        .setQuery(QueryBuilders.matchAllQuery()).setSize(100).execute()
        .actionGet();

    int fieldnum = fields.size();
    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> metadata = hit.getSource();

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

  /**
   * load code values of giving variables
   *
   * @param es
   *          the Elasticsearch client
   * @param fields
   *          variables list
   * @return a map from variable value to code
   */
  public Map<String, Vector> loadFieldsOHEncodeMap(ESDriver es,
      List<String> fields) {

    Map<String, Vector> metedataCode = new HashMap<>();
    SearchResponse scrollResp = es.getClient().prepareSearch(indexName)
        .setTypes(metadataType).setScroll(new TimeValue(60000))
        .setQuery(QueryBuilders.matchAllQuery()).setSize(100).execute()
        .actionGet();

    int fieldnum = fields.size();
    OHEncoder coder = new OHEncoder();
    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> metadata = hit.getSource();

        String shortname = (String) metadata.get("Dataset-ShortName");

        double[] codeArr = null;
        for (int i = 0; i < fieldnum; i++) {
          String field = fields.get(i);
          String code = (String) metadata.get(field + "_code");
          String[] values = code.split(",");
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
}
