package esiptestbed.mudrod.discoveryengine;

import java.util.Map;

import esiptestbed.mudrod.driver.ESDriver;

public abstract class DiscoveryStepAbstract extends MudrodAbstract {

	public DiscoveryStepAbstract(Map<String, String> config, ESDriver es) {
		super(config, es);
		// TODO Auto-generated constructor stub
	}
	
	public abstract Object execute();
	
	public abstract Object execute(Object o);

}
