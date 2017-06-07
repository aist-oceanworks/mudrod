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
package gov.nasa.jpl.mudrod.ssearch;

import gov.nasa.jpl.mudrod.discoveryengine.MudrodAbstract;
import gov.nasa.jpl.mudrod.driver.ESDriver;
import gov.nasa.jpl.mudrod.driver.SparkDriver;
import gov.nasa.jpl.mudrod.integration.LinkageIntegration;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * Supports ability to transform regular user query into a semantic query
 */
public class Dispatcher extends MudrodAbstract {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  public Dispatcher(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  /**
   * Method of getting semantically most related terms by number
   *
   * @param input regular input query
   * @param num the number of most related terms
   * @return a map from term to similarity
   */
  public Map<String, Double> getRelatedTerms(String input, int num) {
    LinkageIntegration li = new LinkageIntegration(props, this.es, null);
    Map<String, Double> sortedMap = li.appyMajorRule(input);
    Map<String, Double> selectedMap = new HashMap<>();
    int count = 0;
    for (Entry<String, Double> entry : sortedMap.entrySet()) {
      if (count < num) {
        selectedMap.put(entry.getKey(), entry.getValue());
      }
      count++;
    }
    return selectedMap;
  }

  /**
   * Method of getting semantically most related terms by similarity threshold
   *
   * @param input regular input query
   * @param threshold value of threshold, raning from 0 to 1
   * @return a map from term to similarity
   */
  public Map<String, Double> getRelatedTermsByT(String input, double threshold) {
    LinkageIntegration li = new LinkageIntegration(this.props, this.es, null);
    Map<String, Double> sortedMap = li.appyMajorRule(input);
    Map<String, Double> selectedMap = new HashMap<>();

    for (Entry<String, Double> entry : sortedMap.entrySet()) {
      if (entry.getValue() >= threshold) {
        selectedMap.put(entry.getKey(), entry.getValue());
      }
    }
    return selectedMap;
  }

  /**
   * Method of creating semantic query based on Threshold
   *
   * @param input regular query
   * @param threshold threshold raning from 0 to 1
   * @param queryOperator query mode
   * @return a multiMatch query builder
   */
  public BoolQueryBuilder createSemQuery(String input, double threshold, String queryOperator) {
    Map<String, Double> selectedMap = getRelatedTermsByT(input, threshold);
    selectedMap.put(input, (double) 1);

    String[] fieldsList = { 
            "Dataset-Metadata",
            "Dataset-ShortName",
            "Dataset-ShortName-Full",
            "Dataset-LongName",
            "Dataset-LongName-Full",
            "DatasetParameter-Topic",
            "DatasetParameter-VariableDetail",
            "DatasetParameter-Category",
            "DatasetParameter-Variable",
            "DatasetParameter-Variable-Full" ,
            "DatasetParameter-Term",
            "DatasetParameter-Term-Full",
            "DatasetSource-Source-LongName",
            "DatasetSource-Source-LongName-Full",
            "DatasetSource-Source-ShortName",
            "DatasetSource-Source-ShortName-Full", 
            "DatasetSource-Sensor-LongName",
            "DatasetSource-Sensor-LongName-Full",
            "DatasetSource-Sensor-ShortName",
            "DatasetSource-Sensor-ShortName-Full",
            "Dataset-Description",
            "Dataset-ProviderDatasetName",
            "Dataset-Doi",
            "Dataset-TemporalResolution-Group",
            "Dataset-PersistentId",
            "DatasetCitation-SeriesName",
            "DatasetCitation-Title",
            "DatasetProject-Project-LongName",
            "DatasetProject-Project-LongName-Full",
            "DatasetProject-Project-ShortName",
            "DatasetProject-Project-ShortName-Full",
            "Collection-LongName",
            "Collection-LongName-Full",
            "Collection-ShortName",
            "Collection-ShortName-Full",
    };

    BoolQueryBuilder qb = new BoolQueryBuilder();
    for (Entry<String, Double> entry : selectedMap.entrySet()) {
      if ("phrase".equals(queryOperator.toLowerCase().trim())) {
        qb.should(QueryBuilders.multiMatchQuery(entry.getKey(), fieldsList).boost(entry.getValue().floatValue()).type(MultiMatchQueryBuilder.Type.PHRASE).tieBreaker((float) 0.5)); // when
        // set to 1.0, it would be equal to "most fields" query
      } else if ("and".equals(queryOperator.toLowerCase().trim())) {
        qb.should(QueryBuilders.multiMatchQuery(entry.getKey(), fieldsList).boost(entry.getValue().floatValue()).operator(MatchQueryBuilder.DEFAULT_OPERATOR.AND).tieBreaker((float) 0.5));
      } else {
        qb.should(QueryBuilders.multiMatchQuery(entry.getKey(), fieldsList).boost(entry.getValue().floatValue()).operator(MatchQueryBuilder.DEFAULT_OPERATOR.OR).tieBreaker((float) 0.5));
      }
    }

    return qb;
  }

}
