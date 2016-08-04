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
package esiptestbed.mudrod.ssearch.structure;

import java.lang.reflect.Field;

public class SResult {
//may replace it with Map<String,Object>
  String shortName = null;
  String longName = null;
  String topic = null;
  String description = null;
  String relase_date = null;

  public long dateLong = 0;
  public Double clicks = null;
  public Double relevance = null;

  public Double final_score = 0.0;
  public Double term_score = 0.0;
  public Double releaseDate_score = 0.0;
  public Double click_score = 0.0;
  public Double allPop_score = 0.0;
  
  public String version = null;
  public String processingLevel = null;
  public String latency = null;
  public String stopDateLong = null;
  public String stopDateFormat = null;
  public Double spatialR = null;
  public String temporalR = null;
  public Integer userPop = null;
  public Integer allPop = null;
  public Integer monthPop = null;

  public SResult(String shortName, String longName, String topic, String description, String date){
    this.shortName = shortName;
    this.longName = longName;
    this.topic = topic;
    this.description = description;
    this.relase_date = date;
  }
  
  public static String getHeader(String delimiter){
     return "ShortName" + delimiter +
            
             "final_score" + delimiter + 
             "term_score" + delimiter + 
             "click_score" + delimiter + 
             "clicks" + delimiter +
             "releaseDate_score" + delimiter + 
         
            /*"Version"  + delimiter +
            "Processing Level"  + delimiter +
            "Latency(hrs)"  + delimiter +
            "StopDate(long)"  + delimiter +
            "StopDate(format)"  + delimiter +
            "SpatialResolution(grid)"  + delimiter +
            "TemporalResolution"  + delimiter +
            "UserPopularity"  + delimiter +*/
            "ReleaseDate(format)" + delimiter +
            "AllPopularity"  + delimiter +
            "AllPop_score"  + delimiter +
           /* "MonthPopularity"  + delimiter +
            "ReleaseDate(long)"  + delimiter +*/            
            "Evaluation"  + delimiter +
            "Comments"  + delimiter +
            "\n";
  }
  
  public String toString(String delimiter ){
    return shortName + delimiter + 
        
           final_score + delimiter + 
           term_score + delimiter + 
           click_score + delimiter + 
           clicks + delimiter +
           releaseDate_score + delimiter +
        
          /* version + delimiter + 
           processingLevel + delimiter + 
           latency + delimiter + 
           stopDateLong + delimiter +
           stopDateFormat + delimiter +
           spatialR + delimiter + 
           temporalR + delimiter + 
           userPop + delimiter + */
           relase_date + delimiter +
           allPop + delimiter + 
           allPop_score + delimiter +
           /*monthPop + delimiter + 
           dateLong + delimiter + */
           "\n";
  }

  public static boolean set(Object object, String fieldName, Object fieldValue) {
    Class<?> clazz = object.getClass();
    while (clazz != null) {
      try {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(object, fieldValue);
        return true;
      } catch (NoSuchFieldException e) {
        clazz = clazz.getSuperclass();
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  public static <V> V get(Object object, String fieldName) {
    Class<?> clazz = object.getClass();
    while (clazz != null) {
      try {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        return (V) field.get(object);
      } catch (NoSuchFieldException e) {
        clazz = clazz.getSuperclass();
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }
    return null;
  }

}
