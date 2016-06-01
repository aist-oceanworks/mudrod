package esiptestbed.mudrod.discoveryengine;

import java.io.Serializable;
import java.util.Map;

import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;

public abstract class DiscoveryEngineAbstract extends MudrodAbstract implements Serializable {
	public DiscoveryEngineAbstract(Map<String, String> config, ESDriver es, SparkDriver spark) {
		super(config, es, spark);
		// TODO Auto-generated constructor stub
	}
	
	public abstract void preprocess();
	public abstract void process();
	public abstract void output();
}
