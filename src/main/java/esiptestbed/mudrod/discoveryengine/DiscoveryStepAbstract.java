package esiptestbed.mudrod.discoveryengine;

import java.io.Serializable;
import java.util.Map;

import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;

public abstract class DiscoveryStepAbstract extends MudrodAbstract{

	public DiscoveryStepAbstract(Map<String, String> config, ESDriver es, SparkDriver spark) {
		super(config, es, spark);
		// TODO Auto-generated constructor stub
	}
	
	public abstract Object execute();
	
	public abstract Object execute(Object o);

}
