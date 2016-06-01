package esiptestbed.mudrod.discoveryengine;

import java.io.Serializable;
import java.util.Map;

import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.metadata.MetadataSVDAnalyzer;

public class MetadataDiscoveryEngine extends DiscoveryEngineAbstract implements Serializable {	
	
	public MetadataDiscoveryEngine(Map<String, String> config, ESDriver es, SparkDriver spark) {
		super(config, es, spark);
		// TODO Auto-generated constructor stub
	}
	
	public void preprocess() {
		// TODO Auto-generated method stub

	}

	public void process() {
		// TODO Auto-generated method stub
		print("*****************metadata processing starts******************", 3);

		DiscoveryStepAbstract svd = new MetadataSVDAnalyzer(this.config, this.es, this.spark);
		svd.execute();
		
		endTime=System.currentTimeMillis();
		System.out.println("*****************metadata processing ends******************Took " + (endTime-startTime)/1000+"s");
	}

	public void output() {
		// TODO Auto-generated method stub

	}
}
