package esiptestbed.mudrod.recommendation.process;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.recommendation.structure.ModelBasedRating;
import esiptestbed.mudrod.semantics.SemanticAnalyzer;
import esiptestbed.mudrod.utils.LinkageTriple;
import scala.Tuple2;

public class ModelBasedCF extends DiscoveryStepAbstract implements Serializable {

	public ModelBasedCF(Map<String, String> config, ESDriver es, SparkDriver spark) {
		super(config, es, spark);
		// TODO Auto-generated constructor stub
	}

	@Override
	public Object execute() {
		// TODO Auto-generated method stub
		
		try {
			ModelBasedRating mbrate = new ModelBasedRating();
			mbrate.predictRating(es, spark.sc, config.get("user_item_rate"));
			
			String MBPredictionFileName = config.get("mb_predictionMatrix");
			mbrate.saveToCSV(MBPredictionFileName);
			
			SemanticAnalyzer analyzer = new SemanticAnalyzer(config, es, spark);
			List<LinkageTriple> triples = analyzer.CalTermSimfromMatrix(MBPredictionFileName);
			analyzer.SaveToES(triples, config.get("indexName"), config.get("metadataModelBasedSimType"));
			
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
