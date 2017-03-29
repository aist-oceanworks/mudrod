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
package gov.nasa.jpl.mudrod.utils;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Iterator;
import java.util.List;

/**
 * ClassName: RDDUtil Function: Mudrod Spark RDD common methods
 */
public class RDDUtil {

  public RDDUtil() {
  }

  /**
   * getAllWordsInDoc: Extracted all unique terms from all docs.
   *
   * @param docwordRDD Pair RDD, each key is a doc, and value is term list extracted from
   *                   that doc.
   * @return unique term list
   */
  public static JavaRDD<String> getAllWordsInDoc(JavaPairRDD<String, List<String>> docwordRDD) {
    JavaRDD<String> wordRDD = docwordRDD.values().flatMap(new FlatMapFunction<List<String>, String>() {
      /**
       *
       */
      private static final long serialVersionUID = 1L;

      @Override
      public Iterator<String> call(List<String> list) {
        return list.iterator();
      }
    }).distinct();

    return wordRDD;
  }
}
