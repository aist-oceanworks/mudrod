package esiptestbed.mudrod.discoveryengine;

import java.util.Map;

import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.recommendation.pre.ApiHarvester;
import esiptestbed.mudrod.recommendation.pre.OBEncoding;


public class RecommendEngine extends DiscoveryEngineAbstract {

	public RecommendEngine(Map<String, String> config, ESDriver es, SparkDriver spark) {
		super(config, es, spark);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void preprocess() {
		// TODO Auto-generated method stub
		System.out.println("*****************Metadata preprocessing starts******************");
	    startTime = System.currentTimeMillis();

	   /* DiscoveryStepAbstract harvester = new ApiHarvester(this.config, this.es,
	        this.spark);
	    harvester.execute();*/
	  
	    DiscoveryStepAbstract obencoder = new OBEncoding(this.config, this.es,
		        this.spark);
	    obencoder.execute();

	    endTime = System.currentTimeMillis();
	    System.out.println("*****************Metadata preprocessing ends******************Took "+ (endTime - startTime) / 1000);
	}

	@Override
	public void process() {
		// TODO Auto-generated method stub

	}

	@Override
	public void output() {
		// TODO Auto-generated method stub

	}

}
