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
		JavaPairRDD<String, Vector> importRDD = MatrixUtil.loadVectorFromCSV(spark, config.get("userHistoryOutputfile"), 2);
		CoordinateMatrix simMatrix = SimilarityUtil.CalSimilarityFromVector(importRDD.values());
		List<LinkageTriple> triples= SimilarityUtil.MatrixtoTriples(importRDD.keys(), simMatrix);
		try {
			LinkageTriple.insertTriples(es, triples, config.get("indexName"), config.get("userHistoryLinkageType"));
		} catch (IOException e) {
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
