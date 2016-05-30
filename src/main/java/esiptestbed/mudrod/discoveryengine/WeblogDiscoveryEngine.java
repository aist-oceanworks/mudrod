package esiptestbed.mudrod.discoveryengine;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.index.query.QueryBuilders;

import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.weblog.CrawlerDetection;
import esiptestbed.mudrod.weblog.ImportLogFile;
import esiptestbed.mudrod.weblog.SessionGenerator;
import esiptestbed.mudrod.weblog.SessionStatistic;

public class WeblogDiscoveryEngine extends DiscoveryEngineAbstract {		
	public WeblogDiscoveryEngine(Map<String, String> config, ESDriver es){
		super(config, es);
	}
	
	public void preprocess() {
		// TODO Auto-generated method stub	
		long startTime, endTime;
		System.out.println("*****************Preprocess begines******************");
		long startTime_preprocess=System.currentTimeMillis();
		
		/*System.out.println("*****************Step 1: Import begines******************");
		startTime=System.currentTimeMillis();
		ImportLogFile imp = new ImportLogFile(this.config, this.es);
		imp.readFile();
		endTime=System.currentTimeMillis();
		System.out.println("*****************Import ends******************Took " + (endTime-startTime)/1000+"s");*/
		
		System.out.println("*****************Crawler detection begines******************");
		startTime=System.currentTimeMillis();
		CrawlerDetection crawlerDet = new CrawlerDetection(this.config, this.es);
		try {
			crawlerDet.CheckByRate();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		endTime=System.currentTimeMillis();
		System.out.println("*****************Crawler detection ends******************Took " + (endTime-startTime)/1000+"s");
		
		System.out.println("*****************Session generating begines******************");
		startTime=System.currentTimeMillis();
		SessionGenerator sessiongen = new SessionGenerator(this.config, this.es);
		sessiongen.generateSession();
		endTime=System.currentTimeMillis();
		System.out.println("*****************Session generating ends******************Took " + (endTime-startTime)/1000+"s");
		
		System.out.println("*****************Session summarizing begines******************");
		startTime=System.currentTimeMillis();
		SessionStatistic sta = new SessionStatistic(this.config, this.es);
		try {
			sta.processSession();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		endTime=System.currentTimeMillis();
		System.out.println("*****************Session summarizing ends******************Took " + (endTime-startTime)/1000+"s");
		
		System.out.println("*****************Clean raw log begines******************");
		startTime=System.currentTimeMillis();
		es.deleteAllByQuery(config.get("indexName"), HTTP_type, QueryBuilders.matchAllQuery());
		es.deleteAllByQuery(config.get("indexName"), FTP_type, QueryBuilders.matchAllQuery());
		endTime=System.currentTimeMillis();
		System.out.println("*****************Clean raw log ends******************Took " + (endTime-startTime)/1000+"s");
		
		long endTime_preprocess=System.currentTimeMillis();
		System.out.println("*****************Preprocessing ends******************Took " + (endTime_preprocess-startTime_preprocess)/1000+"s");

	}

	public void process() {
		// TODO Auto-generated method stub

	}

	public void output() {
		// TODO Auto-generated method stub

	}

}
