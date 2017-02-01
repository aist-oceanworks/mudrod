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
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;

import scala.Tuple2;

/**
 * ClassName: SimilarityUtil Function: Similarity calculation tool 
 */
public class SimilarityUtil {

  public static final int SIM_PEARSON = 1;
  public static final int SIM_HELLINGER = 2;
  public static final int SIM_COSINE = 3;

  /**
   * CalSimilarityFromMatrix: Calculate term similarity from matrix.
   *
   * @param svdMatrix.
   *          Each row is corresponding to a term, and each column is
   *          corresponding to a dimension of feature
   * @return CoordinateMatrix, each row is corresponding to a term, and each
   *         column is also a term, the cell value is the similarity between the
   *         two terms
   */
  public static CoordinateMatrix CalSimilarityFromMatrix(RowMatrix svdMatrix) {
    JavaRDD<Vector> vecs = svdMatrix.rows().toJavaRDD();
    return SimilarityUtil.CalSimilarityFromVector(vecs);
  }

  /**
   * CalSimilarityFromVector:Calculate term similarity from vector.
   *
   * @param vecs
   *          Each vector is corresponding to a term in the feature space.
   * @return CoordinateMatrix, each row is corresponding to a term, and each
   *         column is also a term, the cell value is the similarity between the
   *         two terms
   */
  public static CoordinateMatrix CalSimilarityFromVector(JavaRDD<Vector> vecs) {
    IndexedRowMatrix indexedMatrix = MatrixUtil.buildIndexRowMatrix(vecs);
    RowMatrix transposeMatrix = MatrixUtil.transposeMatrix(indexedMatrix);
    CoordinateMatrix simMatirx = transposeMatrix.columnSimilarities();
    return simMatirx;
  }

  /**
   * CalSimilarityFromVector:Calculate term similarity from vector.
   *
   * @param vecs
   *          Each vector is corresponding to a term in the feature space.
   * @return
   */
  public static JavaRDD<LinkageTriple> CalSimilarityFromVector(
      JavaPairRDD<String, Vector> importRDD, int simType) {
    JavaRDD<Tuple2<String, Vector>> importRDD1 = importRDD
        .map(f -> new Tuple2<String, Vector>(f._1, f._2));
    JavaPairRDD<Tuple2<String, Vector>, Tuple2<String, Vector>> cartesianRDD = importRDD1
        .cartesian(importRDD1);

    JavaRDD<LinkageTriple> tripleRDD = cartesianRDD.map(
        new Function<Tuple2<Tuple2<String, Vector>, Tuple2<String, Vector>>, LinkageTriple>() {

          @Override
          public LinkageTriple call(
              Tuple2<Tuple2<String, Vector>, Tuple2<String, Vector>> arg) {
            String keyA = arg._1._1;
            String keyB = arg._2._1;

            if (keyA.equals(keyB)) {
              return null;
            }

            Vector vecA = arg._1._2;
            Vector vecB = arg._2._2;
            Double weight = 0.0;

            if (simType == SimilarityUtil.SIM_PEARSON) {
              weight = SimilarityUtil.pearson_distance(vecA, vecB);
            } else if (simType == SimilarityUtil.SIM_HELLINGER) {
              weight = SimilarityUtil.hellinger_distance(vecA, vecB);
            }

            LinkageTriple triple = new LinkageTriple();
            triple.keyA = keyA;
            triple.keyB = keyB;
            triple.weight = weight;
            return triple;
          }
        }).filter(new Function<LinkageTriple, Boolean>() {
          @Override
          public Boolean call(LinkageTriple arg0) throws Exception {
            if (arg0 == null) {
              return false;
            }
            return true;
          }
        });

    return tripleRDD;
  }

  /**
   * MatrixtoTriples:Convert term similarity matrix to linkage triple list.
   *
   *
   * @param keys
   *          each key is a term
   * @param simMatirx
   *          term similarity matrix, in which each row and column is a term and
   *          the cell value is the similarity between the two terms
   * @return linkage triple list
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
              /**
               * 
               */
              private static final long serialVersionUID = 1L;

              @Override
              public Tuple2<Long, String> call(Tuple2<String, Long> doc_id) {
                return doc_id.swap();
              }
            }));

    JavaPairRDD<Long, LinkageTriple> entries_rowRDD = simMatirx.entries()
        .toJavaRDD()
        .mapToPair(new PairFunction<MatrixEntry, Long, LinkageTriple>() {
          /**
           * 
           */
          private static final long serialVersionUID = 1L;

          @Override
          public Tuple2<Long, LinkageTriple> call(MatrixEntry t)
              throws Exception {
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
              /**
               * 
               */
              private static final long serialVersionUID = 1L;

              @Override
              public Tuple2<Long, LinkageTriple> call(
                  Tuple2<LinkageTriple, Optional<String>> t) throws Exception {
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
              /**
               * 
               */
              private static final long serialVersionUID = 1L;

              @Override
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

  /**
   * calculate similarity between vectors
   *
   * @param vecA
   * @param vecB
   * @return similarity between two vectors
   */
  public static double hellinger_distance(Vector vecA, Vector vecB) {
    double[] arrA = vecA.toArray();
    double[] arrB = vecB.toArray();

    double sim = 0.0;

    int arrsize = arrA.length;
    for (int i = 0; i < arrsize; i++) {
      double a = arrA[i];
      double b = arrB[i];
      double sqrtDiff = Math.sqrt(a) - Math.sqrt(b);
      sim += sqrtDiff * sqrtDiff;
    }

    sim = sim / Math.sqrt(2);

    return sim;
  }

  /**
   * calculate similarity between vectors
   *
   * @param vecA
   * @param vecB
   * @return similarity between two vectors
   */
  public static double pearson_distance(Vector vecA, Vector vecB) {
    double[] arrA = vecA.toArray();
    double[] arrB = vecB.toArray();

    int viewA = 0;
    int viewB = 0;
    int viewAB = 0;

    int arrsize = arrA.length;
    for (int i = 0; i < arrsize; i++) {
      if (arrA[i] > 0) {
        viewA++;
      }

      if (arrB[i] > 0) {
        viewB++;
      }

      if (arrB[i] > 0 && arrA[i] > 0) {
        viewAB++;
      }
    }

    double weight = viewAB / (Math.sqrt(viewA) * Math.sqrt(viewB));

    return weight;
  }

  /**
   * calculate similarity between vectors
   *
   * @param vecA
   * @param vecB
   * @return similarity between two vectors
   */
  public static double cosine_distance(Vector vecA, Vector vecB) {
    return 1;
  }
}