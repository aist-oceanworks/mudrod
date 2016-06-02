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
package esiptestbed.mudrod.weblog.structure;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.weblog.structure.ClickStream;
import scala.Tuple2;

public class SessionExtractor implements Serializable {

	public SessionExtractor() {
		// TODO Auto-generated constructor stub
	}

	// load data from es
		public JavaRDD<ClickStream> extractClickStremFromES(Map<String, String> config, ESDriver es, SparkDriver spark) throws Exception {
			List<ClickStream> QueryList = this.getClickStreamList(config,es);
			JavaRDD<ClickStream> clickstreamRDD = spark.sc.parallelize(QueryList);
			return clickstreamRDD;
		}
		
		protected List<ClickStream> getClickStreamList(Map<String, String> config, ESDriver es) throws Exception {
			List<ClickStream> result = new ArrayList<ClickStream>();
			for (int n = 1; n <= 12; n++) {
				String month = String.format("%02d", n);
				List<String> sessionIds = this.getSessions(config,es,month);
				String cleanupType = config.get("Cleanup_type") + month;
				Session session = new Session(config,es);
				int sessionNum = sessionIds.size();
				for (int i=0; i<sessionNum; i++){
					List<ClickStream> datas = session.getClickStreamList(cleanupType, sessionIds.get(i));
					result.addAll(datas);
				}
			}

			return result;
		}

		// load data from txt file
		public JavaRDD<ClickStream> loadClickStremFromTxt(String clickthroughFile, JavaSparkContext sc) {
			JavaRDD<ClickStream> clickstreamRDD = sc.textFile(clickthroughFile)
					.flatMap(new FlatMapFunction<String, ClickStream>() {
						public Iterable<ClickStream> call(String line) throws Exception {
							List<ClickStream> clickthroughs = (List<ClickStream>) ClickStream.parseFromTextLine(line);
							return clickthroughs;
						}
					});
			return clickstreamRDD;
		}

		public JavaPairRDD<String, List<String>> bulidDataQueryRDD(JavaRDD<ClickStream> clickstreamRDD) {
			JavaPairRDD<String, List<String>> metaDataQueryRDD = clickstreamRDD
					.mapToPair(new PairFunction<ClickStream, String, List<String>>() {
						public Tuple2<String, List<String>> call(ClickStream click) throws Exception {
							List<String> query = new ArrayList<String>();
							query.add(click.getKeyWords());
							return new Tuple2<String, List<String>>(click.getViewDataset(), query);
						}
					}).reduceByKey(new Function2<List<String>, List<String>, List<String>>() {
						public List<String> call(List<String> v1, List<String> v2) throws Exception {
							// TODO Auto-generated method stub
							List<String> list = new ArrayList<String>();
							list.addAll(v1);
							list.addAll(v2);
							return list;
						}
					});

			return metaDataQueryRDD;
		}
		
		protected List<String> getSessions(Map<String, String> config, ESDriver es, String month) throws Exception {
			List<String> sessionIDs = new ArrayList<String>();
			SearchResponse sr = es.client.prepareSearch(config.get("indexName"))                          
					.setTypes(config.get("Cleanup_type") + month)
					.setQuery(QueryBuilders.matchAllQuery())
					.setSize(0)
					.addAggregation(AggregationBuilders.terms("Sessions")
							.field("SessionID").size(0))
					.execute().actionGet();
			Terms Sessions = sr.getAggregations().get("Sessions");
			for (Terms.Bucket entry : Sessions.getBuckets()) {	
				sessionIDs.add(entry.getKey());
			}
			return sessionIDs;
		}
}
