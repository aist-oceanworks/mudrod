/*
 * Licensed under the Apache License, Version 2.0 (the "License"); you 
 * may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package esiptestbed.mudrod.discoveryengine;

import java.util.Map;

import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.weblog.pre.ClickStreamGenerator;
import esiptestbed.mudrod.weblog.pre.CrawlerDetection;
import esiptestbed.mudrod.weblog.pre.HistoryGenerator;
import esiptestbed.mudrod.weblog.pre.ImportLogFile;
import esiptestbed.mudrod.weblog.pre.RemoveRawLog;
import esiptestbed.mudrod.weblog.pre.SessionGenerator;
import esiptestbed.mudrod.weblog.pre.SessionStatistic;
import esiptestbed.mudrod.weblog.process.ClickStreamAnalyzer;
import esiptestbed.mudrod.weblog.process.UserHistoryAnalyzer;


public class WeblogDiscoveryEngine extends DiscoveryEngineAbstract {		
	public WeblogDiscoveryEngine(Map<String, String> config, ESDriver es, SparkDriver spark){
		super(config, es, spark);
	}
	
	@Override
	public void preprocess() {
		// TODO Auto-generated method stub	
		System.out.println("*****************Web log preprocessing starts******************");
		startTime=System.currentTimeMillis();
		
		DiscoveryStepAbstract im = new ImportLogFile(this.config, this.es, this.spark);
		im.execute();
		
		DiscoveryStepAbstract cd = new CrawlerDetection(this.config, this.es, this.spark);
		cd.execute();
		
		DiscoveryStepAbstract sg = new SessionGenerator(this.config, this.es, this.spark);
		sg.execute();
		
		DiscoveryStepAbstract ss = new SessionStatistic(this.config, this.es, this.spark);
		ss.execute();
		
		DiscoveryStepAbstract rr = new RemoveRawLog(this.config, this.es, this.spark);
		rr.execute();
		
		DiscoveryStepAbstract hg = new HistoryGenerator(this.config, this.es, this.spark);
		hg.execute();
		
		DiscoveryStepAbstract cg = new ClickStreamGenerator(this.config, this.es, this.spark);
		cg.execute();
				
		endTime=System.currentTimeMillis();
		System.out.println("*****************Web log preprocessing ends******************Took " + (endTime-startTime)/1000+"s");

	}
	

	@Override
	public void process() {
		// TODO Auto-generated method stub
		System.out.println("*****************Web log processing starts******************");


		DiscoveryStepAbstract svd = new ClickStreamAnalyzer(this.config, this.es, this.spark);
		svd.execute();
		
		DiscoveryStepAbstract ua = new UserHistoryAnalyzer(this.config, this.es, this.spark);
		ua.execute();
		
		endTime=System.currentTimeMillis();
		System.out.println("*****************Web log processing ends******************Took " + (endTime-startTime)/1000+"s");
	}

	@Override
	public void output() {
		// TODO Auto-generated method stub
		
	}

}
