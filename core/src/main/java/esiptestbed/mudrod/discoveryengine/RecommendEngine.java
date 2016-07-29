package esiptestbed.mudrod.discoveryengine;

import java.util.Map;

import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.metadata.process.MetadataAnalyzer;
import esiptestbed.mudrod.recommendation.pre.ApiHarvester;
import esiptestbed.mudrod.recommendation.pre.MatrixGenerator;
import esiptestbed.mudrod.recommendation.pre.OBEncoding;
import esiptestbed.mudrod.recommendation.process.ContentBasedCF;


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
	  
	    /*DiscoveryStepAbstract obencoder = new OBEncoding(this.config, this.es,
		        this.spark);
	    obencoder.execute();*/
	    
	   /* DiscoveryStepAbstract obencoder = new MatrixGenerator(this.config, this.es,
		        this.spark);
	    obencoder.execute();*/

	    endTime = System.currentTimeMillis();
	    System.out.println("*****************Metadata preprocessing ends******************Took "+ (endTime - startTime) / 1000);
	}

	@Override
	public void process() {
		// TODO Auto-generated method stub

		System.out.println("*****************Metadata processing starts******************");
		startTime = System.currentTimeMillis();

		DiscoveryStepAbstract matrix = new ContentBasedCF(this.config, this.es, this.spark);
		matrix.execute();

		endTime = System.currentTimeMillis();
		 System.out.println("*****************Metadata processing ends******************Took "+ (endTime - startTime) / 1000);
	}

	@Override
	public void output() {
		// TODO Auto-generated method stub

	}

}
