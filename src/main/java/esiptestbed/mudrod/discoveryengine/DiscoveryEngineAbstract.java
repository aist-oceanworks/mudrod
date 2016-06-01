package esiptestbed.mudrod.discoveryengine;

import java.util.Map;

import esiptestbed.mudrod.driver.ESDriver;

public abstract class DiscoveryEngineAbstract extends MudrodAbstract {

	public DiscoveryEngineAbstract(Map<String, String> config, ESDriver es) {
		super(config, es);
		// TODO Auto-generated constructor stub
	}
	
	public abstract void preprocess();
	public abstract void process();
	public abstract void output();

}
