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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.feature.IDF;
import org.apache.spark.mllib.feature.IDFModel;
import org.apache.spark.mllib.linalg.Matrices;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.SingularValueDecomposition;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;

import esiptestbed.mudrod.driver.SparkDriver;
import scala.Tuple2;

public class MatrixUtil {

  public static RowMatrix buildSVDMatrix(RowMatrix tfidfMatrix, int dimension)
      throws IOException {
    SingularValueDecomposition<RowMatrix, Matrix> svd = tfidfMatrix
        .computeSVD(dimension, true, 1.0E-9d);
    RowMatrix U = svd.U();
    Vector s = svd.s();

    RowMatrix svdMatrix = U.multiply(Matrices.diag(s));
    return svdMatrix;
  }

  public static RowMatrix buildSVDMatrix(JavaRDD<Vector> vecRDD, int dimension)
      throws IOException {
    RowMatrix tfidfMatrix = new RowMatrix(vecRDD.rdd());
    SingularValueDecomposition<RowMatrix, Matrix> svd = tfidfMatrix
        .computeSVD(dimension, true, 1.0E-9d);
    RowMatrix U = svd.U();
    Vector s = svd.s();

    RowMatrix svdMatrix = U.multiply(Matrices.diag(s));
    return svdMatrix;
  }

  public static RowMatrix createTFIDFMatrix(RowMatrix wordDocMatrix,
      JavaSparkContext sc) {
    JavaRDD<Vector> newcountRDD = wordDocMatrix.rows().toJavaRDD();
    IDFModel idfModel = new IDF().fit(newcountRDD);
    JavaRDD<Vector> idf = idfModel.transform(newcountRDD);
    RowMatrix tfidfMat = new RowMatrix(idf.rdd());

    return tfidfMat;
  }

  public static RowMatrix createWordDocMatrix(
      JavaPairRDD<String, List<String>> uniqueDocRDD, JavaSparkContext sc)
      throws IOException {
    // Index documents with unique IDs
    JavaPairRDD<List<String>, Long> corpus = uniqueDocRDD.values()
        .zipWithIndex();
    // cal word-doc numbers
    JavaPairRDD<Tuple2<String, Long>, Double> worddoc_num_RDD = corpus
        .flatMapToPair(
            new PairFlatMapFunction<Tuple2<List<String>, Long>, Tuple2<String, Long>, Double>() {
              public Iterable<Tuple2<Tuple2<String, Long>, Double>> call(
                  Tuple2<List<String>, Long> docwords) throws Exception {
                List<Tuple2<Tuple2<String, Long>, Double>> pairs = new ArrayList<Tuple2<Tuple2<String, Long>, Double>>();
                List<String> words = docwords._1;
                int n = words.size();
                for (int i = 0; i < n; i++) {
                  Tuple2<String, Long> worddoc = new Tuple2<String, Long>(
                      words.get(i), docwords._2);
                  pairs.add(
                      new Tuple2<Tuple2<String, Long>, Double>(worddoc, 1.0));
                }
                return pairs;
              }
            })
        .reduceByKey(new Function2<Double, Double, Double>() {
          public Double call(Double first, Double second) throws Exception {
            return first + second;
          }
        });
    // cal word doc-numbers
    JavaPairRDD<String, Tuple2<List<Long>, List<Double>>> word_docnum_RDD = worddoc_num_RDD
        .mapToPair(
            new PairFunction<Tuple2<Tuple2<String, Long>, Double>, String, Tuple2<List<Long>, List<Double>>>() {
              public Tuple2<String, Tuple2<List<Long>, List<Double>>> call(
                  Tuple2<Tuple2<String, Long>, Double> worddoc_num)
                  throws Exception {
                List<Long> docs = new ArrayList<Long>();
                docs.add(worddoc_num._1._2);
                List<Double> nums = new ArrayList<Double>();
                nums.add(worddoc_num._2);
                Tuple2<List<Long>, List<Double>> docmums = new Tuple2<List<Long>, List<Double>>(
                    docs, nums);
                return new Tuple2<String, Tuple2<List<Long>, List<Double>>>(
                    worddoc_num._1._1, docmums);
              }
            });
    // trans to vector
    final int corporsize = (int) uniqueDocRDD.keys().count();
    JavaPairRDD<String, Vector> word_vectorRDD = word_docnum_RDD.reduceByKey(
        new Function2<Tuple2<List<Long>, List<Double>>, Tuple2<List<Long>, List<Double>>, Tuple2<List<Long>, List<Double>>>() {
          public Tuple2<List<Long>, List<Double>> call(
              Tuple2<List<Long>, List<Double>> arg0,
              Tuple2<List<Long>, List<Double>> arg1) throws Exception {
            arg0._1.addAll(arg1._1);
            arg0._2.addAll(arg1._2);
            return new Tuple2<List<Long>, List<Double>>(arg0._1, arg0._2);
          }
        }).mapToPair(
            new PairFunction<Tuple2<String, Tuple2<List<Long>, List<Double>>>, String, Vector>() {
              public Tuple2<String, Vector> call(
                  Tuple2<String, Tuple2<List<Long>, List<Double>>> arg0)
                  throws Exception {
                // TODO Auto-generated method stub
                int docsize = arg0._2._1.size();
                int[] intArray = new int[docsize];
                double[] doubleArray = new double[docsize];
                for (int i = 0; i < docsize; i++) {
                  intArray[i] = arg0._2._1.get(i).intValue();
                  doubleArray[i] = arg0._2._2.get(i).intValue();
                }
                Vector sv = Vectors.sparse(corporsize, intArray, doubleArray);
                return new Tuple2<String, Vector>(arg0._1, sv);
              }
            });

    RowMatrix wordDocMatrix = new RowMatrix(word_vectorRDD.values().rdd());
    return wordDocMatrix;
  }

  public static JavaPairRDD<String, Vector> loadVectorFromCSV(SparkDriver spark,
      String csvFileName, int skipNum) {
    // skip the first two lines(header), important!
    JavaRDD<String> importRDD = spark.sc.textFile(csvFileName);
    JavaPairRDD<String, Long> importIdRDD = importRDD.zipWithIndex()
        .filter(new Function<Tuple2<String, Long>, Boolean>() {
          public Boolean call(Tuple2<String, Long> v1) throws Exception {
            if (v1._2 > (skipNum - 1)) {
              return true;
            }
            return false;
          }
        });

    JavaPairRDD<String, Vector> vecRDD = importIdRDD
        .mapToPair(new PairFunction<Tuple2<String, Long>, String, Vector>() {
          public Tuple2<String, Vector> call(Tuple2<String, Long> t)
              throws Exception {
            String[] fields = t._1.split(",");
            String word = fields[0];
            int fieldsize = fields.length;
            String[] numfields = Arrays.copyOfRange(fields, 1, fieldsize - 1);
            double[] nums = Stream.of(numfields)
                .mapToDouble(Double::parseDouble).toArray();
            Vector vec = Vectors.dense(nums);
            return new Tuple2<String, Vector>(word, vec);
          }
        });

    return vecRDD;
  }

  public static IndexedRowMatrix buildIndexRowMatrix(JavaRDD<Vector> vecs) {
    JavaRDD<IndexedRow> indexrows = vecs.zipWithIndex()
        .map(new Function<Tuple2<Vector, Long>, IndexedRow>() {
          public IndexedRow call(Tuple2<Vector, Long> doc_id) {
            return new IndexedRow(doc_id._2, doc_id._1);
          }
        });
    IndexedRowMatrix indexedMatrix = new IndexedRowMatrix(indexrows.rdd());
    return indexedMatrix;
  }

  public static RowMatrix transposeMatrix(IndexedRowMatrix indexedMatrix) {
    RowMatrix transposeMatrix = indexedMatrix.toCoordinateMatrix().transpose()
        .toRowMatrix();
    return transposeMatrix;
  }

  public static void exportToCSV(RowMatrix matrix, List<String> rowKeys,
      List<String> colKeys, String fileName) throws IOException {
    int rownum = (int) matrix.numRows();
    int colnum = (int) matrix.numCols();
    List<Vector> rows = (List<Vector>) matrix.rows().toJavaRDD().collect();

    File file = new File(fileName);
    if (file.exists()) {
      file.delete();
    }
    file.createNewFile();

    FileWriter fw = new FileWriter(file.getAbsoluteFile());
    BufferedWriter bw = new BufferedWriter(fw);
    String coltitle = " Num" + ",";
    for (int j = 0; j < colnum; j++) {
      coltitle += colKeys.get(j) + ",";
    }
    coltitle = coltitle.substring(0, coltitle.length() - 1);
    bw.write(coltitle + "\n");

    /*
     * String secondtitle = " Num" + ","; for (int j = 0; j < colnum; j++) {
     * secondtitle += "f" + j + ","; } secondtitle = secondtitle.substring(0,
     * secondtitle.length() - 1); bw.write(secondtitle + "\n");
     */

    for (int i = 0; i < rownum; i++) {
      double[] rowvlaue = rows.get(i).toArray();
      String row = rowKeys.get(i) + ",";
      for (int j = 0; j < colnum; j++) {
        row += rowvlaue[j] + ",";
      }
      row = row.substring(0, row.length() - 1);
      bw.write(row + "\n");
    }

    bw.close();
  }
}
