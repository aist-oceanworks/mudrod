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
package esiptestbed.mudrod.weblog.structure;

public class KeywordPairData {
  public String keywordA;
  public String keywordB;
  public double weight;
  public String time;
  public String sessionID;

  public String Aurl;
  public String Burl;
  public String viewurl;
  public int type; // 0-single; 1-combo


  public KeywordPairData(){

  }

  public KeywordPairData(String keywordA, String keywordB, double weight){
    this.keywordA = keywordA;
    this.keywordB = keywordB;
    this.weight = weight;
  }

  public KeywordPairData(String keywordA, String keywordB, double weight,int type, String Aurl, String Burl,String viewurl){
    this.keywordA = keywordA;
    this.keywordB = keywordB;
    this.weight = weight;
    //this.sessionID = sessionID;
    this.type = type;

    this.Aurl = Aurl;
    this.Burl = Burl;
    this.viewurl = viewurl;
  }


  public void setSessionId(String sessionID){
    this.sessionID = sessionID;
  }

  public void setTime(String time){
    this.time = time;
  }


  public String toString(){

    return "keywordA:" + keywordA + "|| keywordB" + keywordB + "|| weight:" + weight;

  }

  public String toJson() {

    String jsonQuery = "{";
    jsonQuery += "\"keywordA\":\"" + this.keywordA + "\",";
    jsonQuery += "\"keywordB\":\"" + this.keywordB + "\",";
    jsonQuery += "\"weight\":\"" + this.weight + "\",";
    jsonQuery += "\"sessionId\":\"" + this.sessionID + "\"";

    jsonQuery += "},";

    return jsonQuery;
  }
}
