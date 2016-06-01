package esiptestbed.mudrod.discoveryengine;

import java.util.Map;

import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.ontology.OntologyLinkCal;
import esiptestbed.mudrod.ontology.pre.AggregateTriples;


public class OntologyDiscoveryEngine extends DiscoveryEngineAbstract {
	
	public OntologyDiscoveryEngine(Map<String, String> config, ESDriver es, SparkDriver spark) {
		super(config, es, spark);
		// TODO Auto-generated constructor stub
	}

	public void preprocess() {
		// TODO Auto-generated method stub
		System.out.println("*****************Preprocess starts******************");
		startTime=System.currentTimeMillis();
		
		DiscoveryStepAbstract at = new AggregateTriples(this.config, this.es,this.spark);
		at.execute();
		
		endTime=System.currentTimeMillis();
		System.out.println("*****************Preprocessing ends******************Took " + (endTime-startTime)/1000+"s");
	}

	public void process() {
		// TODO Auto-generated method stub
		System.out.println("*****************Processing starts******************");
		startTime=System.currentTimeMillis();
		
		DiscoveryStepAbstract ol = new OntologyLinkCal(this.config, this.es, this.spark);
		ol.execute();
		
		endTime=System.currentTimeMillis();
		System.out.println("*****************Processing starts******************Took " + (endTime-startTime)/1000+"s");
	}

	public void output() {
		// TODO Auto-generated method stub

	}

}
