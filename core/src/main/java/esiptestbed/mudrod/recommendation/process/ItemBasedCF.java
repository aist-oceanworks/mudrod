package esiptestbed.mudrod.recommendation.process;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.recommendation.structure.ItemSimCalculator;
import esiptestbed.mudrod.semantics.SemanticAnalyzer;
import esiptestbed.mudrod.utils.LinkageTriple;

public class ItemBasedCF extends DiscoveryStepAbstract {

	public ItemBasedCF(Map<String, String> config, ESDriver es, SparkDriver spark) {
		super(config, es, spark);
		// TODO Auto-generated constructor stub
	}

	public Object execute1() {
		// TODO Auto-generated method stub
		try {
			
			String user_metadat_optFile = config.get("user_based_item_optMatrix");
			SemanticAnalyzer analyzer = new SemanticAnalyzer(config, es, spark);
			List<LinkageTriple> triples = analyzer.CalTermSimfromMatrix(user_metadat_optFile);
			analyzer.SaveToES(triples, config.get("indexName"), config.get("metadataItemBasedSimType"));
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public Object execute() {
		// TODO Auto-generated method stub
		try {
			String user_metadat_optFile = config.get("user_based_item_optMatrix");
			ItemSimCalculator simcal = new ItemSimCalculator(config);
			List<LinkageTriple> triples = simcal.CalItemSimfromMatrix(spark, user_metadat_optFile, 1);
			LinkageTriple.insertTriples(es, triples, config.get("indexName"), config.get("metadataItemBasedSimType"), true);
			LinkageTriple.standardTriples(es, config.get("indexName"), config.get("metadataItemBasedSimType"));
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
