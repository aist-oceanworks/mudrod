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
package esiptestbed.mudrod.utils;

import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import com.google.common.base.Optional;

import esiptestbed.mudrod.driver.SparkDriver;
import scala.Tuple2;

public class SimilarityUtil {

  public static CoordinateMatrix CalSimilarityFromMatrix(RowMatrix svdMatrix) {
    JavaRDD<Vector> vecs = svdMatrix.rows().toJavaRDD();
    return SimilarityUtil.CalSimilarityFromVector(vecs);
  }

  public static CoordinateMatrix CalSimilarityFromVector(JavaRDD<Vector> vecs) {
    IndexedRowMatrix indexedMatrix = MatrixUtil.buildIndexRowMatrix(vecs);
    RowMatrix transposeMatrix = MatrixUtil.transposeMatrix(indexedMatrix);
    CoordinateMatrix simMatirx = transposeMatrix.columnSimilarities();
    return simMatirx;
  }

  /*
   * public static CoordinateMatrix CalSimilarityFromCSV(SparkDriver spark,
   * String csvfile){ JavaPairRDD<String, Vector> importRDD =
   * MatrixUtil.loadVectorFromCSV(spark, csvfile, 2); CoordinateMatrix simMatrix
   * = SimilarityUtil.CalSimilarityFromVector(importRDD.values()); return
   * simMatrix; }
   */

  public static List<LinkageTriple> MatrixtoTriples(JavaRDD<String> keys,
      CoordinateMatrix simMatirx) {
    if (simMatirx.numCols() != keys.count()) {
      return null;
    }

    // index words
    JavaPairRDD<Long, String> keyIdRDD = JavaPairRDD
        .fromJavaRDD(keys.zipWithIndex()
            .map(new Function<Tuple2<String, Long>, Tuple2<Long, String>>() {
              public Tuple2<Long, String> call(Tuple2<String, Long> doc_id) {
                return doc_id.swap();
              }
            }));

    JavaPairRDD<Long, LinkageTriple> entries_rowRDD = simMatirx.entries()
        .toJavaRDD()
        .mapToPair(new PairFunction<MatrixEntry, Long, LinkageTriple>() {
          public Tuple2<Long, LinkageTriple> call(MatrixEntry t)
              throws Exception {
            // TODO Auto-generated method stub
            LinkageTriple triple = new LinkageTriple();
            triple.keyAId = t.i();
            triple.keyBId = t.j();
            triple.weight = t.value();
            return new Tuple2<Long, LinkageTriple>(triple.keyAId, triple);
          }
        });

    JavaPairRDD<Long, LinkageTriple> entries_colRDD = entries_rowRDD
        .leftOuterJoin(keyIdRDD).values().mapToPair(
            new PairFunction<Tuple2<LinkageTriple, Optional<String>>, Long, LinkageTriple>() {
              public Tuple2<Long, LinkageTriple> call(
                  Tuple2<LinkageTriple, Optional<String>> t) throws Exception {
                // TODO Auto-generated method stub
                LinkageTriple triple = t._1;
                Optional<String> stra = t._2;
                if (stra.isPresent()) {
                  triple.keyA = stra.get();
                }
                return new Tuple2<Long, LinkageTriple>(triple.keyBId, triple);
              }
            });

    JavaRDD<LinkageTriple> tripleRDD = entries_colRDD.leftOuterJoin(keyIdRDD)
        .values().map(
            new Function<Tuple2<LinkageTriple, Optional<String>>, LinkageTriple>() {
              public LinkageTriple call(
                  Tuple2<LinkageTriple, Optional<String>> t) throws Exception {
                LinkageTriple triple = t._1;
                Optional<String> strb = t._2;
                if (strb.isPresent()) {
                  triple.keyB = strb.get();
                }
                return triple;
              }
            });

    List<LinkageTriple> triples = tripleRDD.collect();
    return triples;
  }
}