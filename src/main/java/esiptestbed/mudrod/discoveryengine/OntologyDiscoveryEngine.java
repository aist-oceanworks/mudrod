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
import esiptestbed.mudrod.ontology.pre.AggregateTriples;
import esiptestbed.mudrod.ontology.process.OntologyLinkCal;


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
