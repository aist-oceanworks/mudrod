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
package esiptestbed.mudrod.metadata.structure;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * ClassName: Metadata Function: Metadata setter and getter methods
 */
public class Metadata implements Serializable {

  public static final String fieldsList[] = {
	        "DatasetParameter-Variable",
	        "DatasetParameter-Topic",
	        "DatasetParameter-Term",
	        "DatasetProject-Project-ShortName",
	        "DatasetProject-Project-LongName",
	        "DatasetSource-Source-LongName",
	        "DatasetSource-Source-ShortName",
	        "DatasetSource-Source-Type",
	        "DatasetSource-Sensor-LongName",
	        "DatasetSource-Sensor-ShortName",
	        "Collection-LongName",
	        "Collection-ShortName",
	        "DatasetParameter-VariableDetail",
	        "DatasetParameter-Category",
	        "Dataset-Metadata"};
  // shortname: data set short name
  String shortname;
  private List<String> allterms = new ArrayList<String>();

  public void setAllterms(List<String> list)
  {
	  allterms = list;
  }
  
  public List<String> getAllterms()
  {
	  return allterms;
  }
  

  public Metadata() {
    // TODO Auto-generated constructor stub
  }

  /**
   * Creates a new instance of PODAACMetadata.
   *
   * @param shortname
   *          data set short name
   * @param longname
   *          data set long name
   * @param topics
   *          data set topics
   * @param terms
   *          data set terms
   * @param variables
   *          data set variables
   * @param keywords
   *          data set keywords
   */
  public Metadata(String shortname) {
    this.shortname = shortname;
  }

}
