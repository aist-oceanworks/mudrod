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

/**
 * Data structure class for search result 
 */
public class SResult {
  String shortName = null;
  String longName = null;
  String topic = null;
  String description = null;
  String relase_date = null;

  public Double dateLong = null;
  public Double clicks = null;
  public Double relevance = null;

  public Double final_score = 0.0;
  public Double term_score = 0.0;
  public Double releaseDate_score = 0.0;
  public Double version_score = 0.0;
  public Double processingL_score = 0.0;
  public Double click_score = 0.0;
  public Double allPop_score = 0.0;
  public Double monthPop_score = 0.0;
  public Double userPop_score = 0.0;
  
  public Double Dataset_LongName_score = null;
  public Double Dataset_Metadata_score = null;
  public Double DatasetParameter_Term_score = null; 
  public Double DatasetSource_Source_LongName_score = null;
  public Double DatasetSource_Sensor_LongName_score = null;
  
  public String version = null;
  public String processingLevel = null;
  public String latency = null;
  public String stopDateLong = null;
  public String stopDateFormat = null;
  public Double spatialR_Sat = null;
  public Double spatialR_Grid = null;
  public String temporalR = null;
  
  public Double versionNum = null;
  public Double proNum = null;
  public Double allPop = null;
  public Double monthPop = null;
  public Double userPop = null;
  
  public Double Dataset_LongName = null;
  public Double Dataset_Metadata = null;
  public Double DatasetParameter_Term = null; 
  public Double DatasetSource_Source_LongName = null;
  public Double DatasetSource_Sensor_LongName = null;
  
  public Double prediction = 0.0;
  public String label = null;

  /**
   * @param shortName short name of dataset
   * @param longName long name of dataset
   * @param topic topic of dataset
   * @param description description of dataset
   * @param date release date of dataset
   */
  public SResult(String shortName, String longName, String topic, String description, String date){
    this.shortName = shortName;
    this.longName = longName;
    this.topic = topic;
    this.description = description;
    this.relase_date = date;
  }
  
  /**
   * Method of getting export header
   * @param delimiter the delimiter used to separate strings
   * @return header
   */
  public static String getHeader(String delimiter){
     return  "ShortName" + delimiter +           
             "term_score" + delimiter + 
             
             "Dataset_LongName" + delimiter + 
             "Dataset_Metadata" + delimiter + 
             "DatasetParameter_Term" + delimiter + 
             "DatasetSource_Source_LongName" + delimiter + 
             "DatasetSource_Sensor_LongName" + delimiter +
             
             "click_score" + delimiter + 
             "releaseDate_score" + delimiter + 
         
            "VersionNum_score"  + delimiter +
            "ProLevelNum_score"  + delimiter +
            "AllPop_score"  + delimiter +
            
            "MonthPop_score"  + delimiter +           
            "UserPop_score"  + delimiter +
            "Label"  + 
            "\n";
  }
 
  /**
   * Method of get a search results as string
   * @param delimiter the delimiter used to separate strings
   * @return search result as string
   */
  public String toString(String delimiter ){
    return shortName + delimiter + 
           term_score + delimiter + 
           
           Dataset_LongName_score + delimiter + 
           Dataset_Metadata_score + delimiter + 
           DatasetParameter_Term_score + delimiter + 
           DatasetSource_Source_LongName_score + delimiter + 
           DatasetSource_Sensor_LongName_score + delimiter +
           
           click_score + delimiter + 
           releaseDate_score + delimiter +
           version_score + delimiter +
           processingL_score + delimiter + 
           allPop_score + delimiter +
           monthPop_score + delimiter +
           userPop_score + delimiter +

           label + 
           "\n";
  }

  /**
   * Generic setter method
   * @param object instance of SResult
   * @param fieldName field name that needs to be set on
   * @param fieldValue field value that needs to be set to
   * @return 1 means success, and 0 otherwise
   */
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

  /**
   * Generic getter method
   * @param object instance of SResult
   * @param fieldName field name of search result
   * @return
   */
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
