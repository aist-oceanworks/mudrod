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
package esiptestbed.mudrod.metadata;

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
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.codehaus.jettison.json.JSONObject;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.metadata.structure.MetadataExtractor;
import esiptestbed.mudrod.metadata.structure.PODAACMetadata;
import esiptestbed.mudrod.utils.LinkageTriple;
import esiptestbed.mudrod.utils.MatrixUtil;
import esiptestbed.mudrod.utils.RDDUtil;
import esiptestbed.mudrod.utils.SVDUtil;
import esiptestbed.mudrod.utils.SimilarityUtil;
import scala.Tuple2;

public class MetadataSVDAnalyzer extends DiscoveryStepAbstract implements Serializable {
	private String index;
	private String type;
	public MetadataSVDAnalyzer(Map<String, String> config, ESDriver es, SparkDriver spark) {
		super(config, es, spark);
		// TODO Auto-generated constructor stub
		this.index = config.get("indexName");
		this.type = config.get("metadataType");
	}
	
	public MetadataSVDAnalyzer() {
		super(null, null, null);
	}

	@Override
	public Object execute(Object o) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object execute() {
		// TODO Auto-generated method stub
		try {
			SVDUtil svdUtil = new SVDUtil(config, es, spark);
			
			MetadataExtractor extractor = new MetadataExtractor();
			JavaPairRDD<String, List<String>> metadataTermsRDD = extractor.loadMetadata(this.es, this.spark.sc, config.get("indexName"),config.get("metadataType"));
			int svdDimension = Integer.parseInt(config.get("metadataSVDDimension"));
			
			svdUtil.buildSVDMatrix(metadataTermsRDD,svdDimension);
			svdUtil.CalSimilarity();
			svdUtil.insertLinkageToES(config.get("indexName"),config.get("metadataSimilarity"));

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
	}
}
