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
package esiptestbed.mudrod.weblog;

import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.metadata.structure.MetadataExtractor;
import esiptestbed.mudrod.utils.LinkageTriple;
import esiptestbed.mudrod.utils.MatrixUtil;
import esiptestbed.mudrod.utils.RDDUtil;
import esiptestbed.mudrod.utils.SVDUtil;
import esiptestbed.mudrod.utils.SimilarityUtil;
import esiptestbed.mudrod.weblog.structure.ClickStream;
import esiptestbed.mudrod.weblog.structure.SessionExtractor;

public class ClickStreamSVDAnalyzer extends DiscoveryStepAbstract {

	public ClickStreamSVDAnalyzer(Map<String, String> config, ESDriver es, SparkDriver spark) {
		super(config, es, spark);
		// TODO Auto-generated constructor stub
	}

	@Override
	public Object execute() {
		// TODO Auto-generated method stub
		try {
			SVDUtil svdUtil = new SVDUtil(config, es, spark);
			
			SessionExtractor extractor = new SessionExtractor();
			JavaRDD<ClickStream> clickstreamRDD = extractor.extractClickStremFromES(this.config, this.es, this.spark);
			JavaPairRDD<String, List<String>> metadataQueryRDD = extractor.bulidMetadataQueryRDD(clickstreamRDD);
			int svdDimension = Integer.parseInt(config.get("clickstreamSVDDimension"));
			
			svdUtil.buildSVDMatrix(metadataQueryRDD,svdDimension);
			svdUtil.CalSimilarity();
			svdUtil.insertLinkageToES(config.get("indexName"), config.get("userClickSimilarity"));
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}

	@Override
	public Object execute(Object o) {
		// TODO Auto-generated method stub
		return null;
	}
}
