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
package esiptestbed.mudrod.semantics;

import java.util.List;
import java.util.Map;

import esiptestbed.mudrod.discoveryengine.MudrodAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.utils.LinkageTriple;

public class SemanticAnalyzer extends MudrodAbstract { //remove MUDRODabstract if it does not work

	public SemanticAnalyzer(Map<String, String> config, ESDriver es,
			SparkDriver spark) {
		super(config, es, spark);
		// TODO Auto-generated constructor stub
	}

	public List<LinkageTriple> CalTermSimfromMatrix(String CSV_fileName){
		return null;
		
	}
	
	public void SaveToES(List<LinkageTriple> triple_List){
		
	}
	
	public String GetSVDMatrix(String CSV_fileName, int svdDimention){
		String svd_matrix_fileName = "";
		
		
		return svd_matrix_fileName;
	}

}
