package esiptestbed.mudrod.weblog.process;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.semantics.SemanticAnalyzer;
import esiptestbed.mudrod.utils.LinkageTriple;
import esiptestbed.mudrod.utils.MatrixUtil;
import esiptestbed.mudrod.utils.SimilarityUtil;

public class UserHistoryAnalyzer extends DiscoveryStepAbstract {
	public UserHistoryAnalyzer(Map<String, String> config, ESDriver es,
			SparkDriver spark) {
		super(config, es, spark);
		// TODO Auto-generated constructor stub
	}

	@Override
	public Object execute() {
		// TODO Auto-generated method stub	
		SemanticAnalyzer sa = new SemanticAnalyzer(config, es, spark);
		List<LinkageTriple> triple_List = sa.CalTermSimfromMatrix(config.get("userHistoryMatrix"));
		sa.SaveToES(triple_List, config.get("indexName"), config.get("userHistoryLinkageType"));
		
		return null;
	}

	@Override
	public Object execute(Object o) {
		// TODO Auto-generated method stub
		return null;
	}

}
