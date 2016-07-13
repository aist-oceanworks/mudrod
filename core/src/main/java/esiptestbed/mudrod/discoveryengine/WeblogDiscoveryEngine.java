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

import java.io.File;
import java.util.ArrayList;
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
		
		File directory = new File(config.get("logDir"));

		ArrayList<String> Input_list = new ArrayList<String>();
		// get all the files from a directory
		File[] fList = directory.listFiles();
		for (File file : fList) {
			if (file.isFile()) {

			} else if (file.isDirectory() && file.getName().matches(".*\\d+.*") && file.getName().contains(config.get("httpPrefix"))) {
				Input_list.add(file.getName().replace(config.get("httpPrefix"), ""));
			}
		}

		for(int i =0; i < Input_list.size(); i++){
			startTime=System.currentTimeMillis();
			System.out.println("*****************Web log preprocessing starts******************" + Input_list.get(i));
			
			DiscoveryStepAbstract im = new ImportLogFile(this.config, this.es, this.spark, Input_list.get(i));
			im.execute();

			DiscoveryStepAbstract cd = new CrawlerDetection(this.config, this.es, this.spark);
			cd.execute();

			DiscoveryStepAbstract sg = new SessionGenerator(this.config, this.es, this.spark);
			sg.execute();

			DiscoveryStepAbstract ss = new SessionStatistic(this.config, this.es, this.spark);
			ss.execute();

			DiscoveryStepAbstract rr = new RemoveRawLog(this.config, this.es, this.spark);
			rr.execute();
			
			endTime=System.currentTimeMillis();
			
			System.out.println("*****************Web log preprocessing ends******************Took " + (endTime-startTime)/1000+"s***" + Input_list.get(i));
		}
		
		DiscoveryStepAbstract hg = new HistoryGenerator(this.config, this.es, this.spark);
		hg.execute();

		DiscoveryStepAbstract cg = new ClickStreamGenerator(this.config, this.es, this.spark);
		cg.execute();
		
		System.out.println("*****************Web log preprocessing (user history and clickstream finished) ends******************");

	}


	@Override
	public void process() {
		// TODO Auto-generated method stub
		System.out.println("*****************Web log processing starts******************");
		startTime=System.currentTimeMillis();

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
