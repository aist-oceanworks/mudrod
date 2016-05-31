package esiptestbed.mudrod.metadata.pre;

import java.util.Map;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;

/**
 * Say sth
 * 
 * 
 * 
 * 
 */

public class Example extends DiscoveryStepAbstract {

	public Example(Map<String, String> config, ESDriver es) {
		super(config, es);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void execute() {
		// TODO Auto-generated method stub
		System.out.println("*****************Step 1: Example******************");
		startTime=System.currentTimeMillis();
		es.createBulkProcesser();
        /* Do something */

        		

		es.destroyBulkProcessor();
		endTime=System.currentTimeMillis();
		System.out.println("*****************Example ends******************Took " + (endTime-startTime)/1000+"s");
	}

}
