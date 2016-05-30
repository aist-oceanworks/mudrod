package esiptestbed.mudrod.weblog.pre;

import java.util.Map;

import org.elasticsearch.index.query.QueryBuilders;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;

public class RemoveRawLog extends DiscoveryStepAbstract {

	public RemoveRawLog(Map<String, String> config, ESDriver es) {
		super(config, es);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void execute() {
		// TODO Auto-generated method stub
		System.out.println("*****************Clean raw log starts******************");
		startTime=System.currentTimeMillis();
		es.deleteAllByQuery(config.get("indexName"), HTTP_type, QueryBuilders.matchAllQuery());
		es.deleteAllByQuery(config.get("indexName"), FTP_type, QueryBuilders.matchAllQuery());
		endTime=System.currentTimeMillis();
		System.out.println("*****************Clean raw log ends******************Took " + (endTime-startTime)/1000+"s");
	}

}
