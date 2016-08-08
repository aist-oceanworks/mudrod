package esiptestbed.mudrod.discoveryengine;

import java.util.Map;

import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.metadata.process.MetadataAnalyzer;
import esiptestbed.mudrod.recommendation.pre.ApiHarvester;
import esiptestbed.mudrod.recommendation.pre.ItemRateGenerator;
import esiptestbed.mudrod.recommendation.pre.ItemRateMatrixGenerator;
import esiptestbed.mudrod.recommendation.pre.OHCodeMatrixGenerator;
import esiptestbed.mudrod.recommendation.pre.OHEncodeMetadata;
import esiptestbed.mudrod.recommendation.pre.TranformMetadata;
import esiptestbed.mudrod.recommendation.process.ContentBasedCF;
import esiptestbed.mudrod.recommendation.process.ItemBasedCF;

public class RecommendEngine extends DiscoveryEngineAbstract {

	public RecommendEngine(Map<String, String> config, ESDriver es, SparkDriver spark) {
		super(config, es, spark);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void preprocess() {
		// TODO Auto-generated method stub
		System.out.println("*****************Recommendation preprocessing starts******************");
	    startTime = System.currentTimeMillis();

	    /*DiscoveryStepAbstract harvester = new ApiHarvester(this.config, this.es,
	        this.spark);
	    harvester.execute();
	    
	    DiscoveryStepAbstract obencoder = new TranformMetadata(this.config, this.es,
		        this.spark);
	    obencoder.execute();
	  
	    DiscoveryStepAbstract obencoder = new OHEncodeMetadata(this.config, this.es,
		        this.spark);
	    obencoder.execute();*/
	    
	    DiscoveryStepAbstract matrixGen = new OHCodeMatrixGenerator(this.config, this.es,
		        this.spark);
	    matrixGen.execute();
	    
	    /*DiscoveryStepAbstract rateGen = new ItemRateGenerator(this.config, this.es,this.spark);
	    rateGen.execute();
	    
	    DiscoveryStepAbstract rateMatrixGen = new ItemRateMatrixGenerator(this.config, this.es,this.spark);
	    rateMatrixGen.execute();*/
	    
	    endTime = System.currentTimeMillis();
	    System.out.println("*****************Recommendation preprocessing ends******************Took "+ (endTime - startTime) / 1000);
	}

	@Override
	public void process() {
		// TODO Auto-generated method stub

		System.out.println("*****************Recommendation processing starts******************");
		startTime = System.currentTimeMillis();

		DiscoveryStepAbstract cbCF = new ContentBasedCF(this.config, this.es, this.spark);
		cbCF.execute();

		DiscoveryStepAbstract itemBasedCF = new ItemBasedCF(this.config, this.es,
		        this.spark);
		itemBasedCF.execute();

		endTime = System.currentTimeMillis();
		System.out.println("*****************Recommendation processing ends******************Took "+ (endTime - startTime) / 1000);
	}

	@Override
	public void output() {
		// TODO Auto-generated method stub

	}

}
