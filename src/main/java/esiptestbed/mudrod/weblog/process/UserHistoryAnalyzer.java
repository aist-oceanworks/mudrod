package esiptestbed.mudrod.weblog.process;

import java.util.List;
import java.util.Map;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.semantics.SemanticAnalyzer;
import esiptestbed.mudrod.utils.LinkageTriple;

public class UserHistoryAnalyzer extends DiscoveryStepAbstract {
	public UserHistoryAnalyzer(Map<String, String> config, ESDriver es,
			SparkDriver spark) {
		super(config, es, spark);
		// TODO Auto-generated constructor stub
	}

	@Override
	public Object execute() {
		// TODO Auto-generated method stub	
		System.out.println("*****************UserHistoryAnalyzer starts******************");
		startTime=System.currentTimeMillis();
		
		SemanticAnalyzer sa = new SemanticAnalyzer(config, es, spark);
		List<LinkageTriple> triple_List = sa.CalTermSimfromMatrix(config.get("userHistoryMatrix"));
		sa.SaveToES(triple_List, config.get("indexName"), config.get("userHistoryLinkageType"));
		
		endTime=System.currentTimeMillis();
		es.refreshIndex();
		System.out.println("*****************UserHistoryAnalyzer ends******************Took " + (endTime-startTime)/1000+"s");
		return null;
	}

	@Override
	public Object execute(Object o) {
		// TODO Auto-generated method stub
		return null;
	}

}
