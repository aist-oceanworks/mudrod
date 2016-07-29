package esiptestbed.mudrod.recommendation.process;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.semantics.SVDAnalyzer;
import esiptestbed.mudrod.semantics.SemanticAnalyzer;
import esiptestbed.mudrod.utils.LinkageTriple;

public class ContentBasedCF extends DiscoveryStepAbstract implements Serializable {

	public ContentBasedCF(Map<String, String> config, ESDriver es,
		      SparkDriver spark) {
		// TODO Auto-generated constructor stub
		super(config, es, spark);
	}

	@Override
	public Object execute() {
		// TODO Auto-generated method stub
		SemanticAnalyzer analyzer = new SemanticAnalyzer(config, es, spark);
		String MatrixCodeFileName = config.get("metadataOBCodeMatrix");
		List<LinkageTriple> triples = analyzer.CalTermSimfromMatrix(MatrixCodeFileName);
		analyzer.SaveToES(triples, config.get("indexName"), config.get("metadataCodeSimType"));

		return null;
	}

	@Override
	public Object execute(Object o) {
		// TODO Auto-generated method stub
		return null;
	}

}
