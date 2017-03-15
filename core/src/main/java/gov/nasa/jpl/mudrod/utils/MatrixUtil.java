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

import gov.nasa.jpl.mudrod.driver.SparkDriver;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.mllib.feature.IDF;
import org.apache.spark.mllib.feature.IDFModel;
import org.apache.spark.mllib.linalg.*;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

/**
 * ClassName: MatrixUtil Function: Matrix tool
 */
public class MatrixUtil {

  private MatrixUtil() {
  }

  /**
   * buildSVDMatrix: Generate SVD matrix from TF-IDF matrix. Please make sure
   * the TF-IDF matrix has been already built from the original documents.
   *
   * @param tfidfMatrix, each row is a term and each column is a document name and each
   *                     cell is the TF-IDF value of the term in the corresponding
   *                     document.
   * @param dimension    Column number of the SVD matrix
   * @return RowMatrix, each row is a term and each column is a dimension in the
   * feature space, each cell is value of the term in the corresponding
   * dimension.
   */
  public static RowMatrix buildSVDMatrix(RowMatrix tfidfMatrix, int dimension) {
    int matrixCol = (int) tfidfMatrix.numCols();
    if (matrixCol < dimension) {
      dimension = matrixCol;
    }

    SingularValueDecomposition<RowMatrix, Matrix> svd = tfidfMatrix
        .computeSVD(dimension, true, 1.0E-9d);
    RowMatrix u = svd.U();
    Vector s = svd.s();
    return u.multiply(Matrices.diag(s));
  }

  /**
   * buildSVDMatrix: Generate SVD matrix from Vector RDD.
   *
   * @param vecRDD    vectors of terms in feature space
   * @param dimension Column number of the SVD matrix
   * @return RowMatrix, each row is a term and each column is a dimension in the
   * feature space, each cell is value of the term in the corresponding
   * dimension.
   */
  public static RowMatrix buildSVDMatrix(JavaRDD<Vector> vecRDD,
      int dimension) {
    RowMatrix tfidfMatrix = new RowMatrix(vecRDD.rdd());
    SingularValueDecomposition<RowMatrix, Matrix> svd = tfidfMatrix
        .computeSVD(dimension, true, 1.0E-9d);
    RowMatrix u = svd.U();
    Vector s = svd.s();
    return u.multiply(Matrices.diag(s));
  }

  /**
   * createTFIDFMatrix:Create TF-IDF matrix from word-doc matrix.
   *
   * @param wordDocMatrix, each row is a term, each column is a document name and each cell
   *                       is number of the term in the corresponding document.
   * @param sc             spark context
   * @return RowMatrix, each row is a term and each column is a document name
   * and each cell is the TF-IDF value of the term in the corresponding
   * document.
   */
  public static RowMatrix createTFIDFMatrix(RowMatrix wordDocMatrix,
      JavaSparkContext sc) {
    JavaRDD<Vector> newcountRDD = wordDocMatrix.rows().toJavaRDD();
    IDFModel idfModel = new IDF().fit(newcountRDD);
    JavaRDD<Vector> idf = idfModel.transform(newcountRDD);
    return new RowMatrix(idf.rdd());
  }

  /**
   * createWordDocMatrix:Create matrix from doc-terms JavaPairRDD.
   *
   * @param uniqueDocRDD doc-terms JavaPairRDD, in which each key is a doc name, and value
   *                     is term list extracted from that doc
   * @param sc           spark context
   * @return LabeledRowMatrix {@link LabeledRowMatrix}
   */
  public static LabeledRowMatrix createWordDocMatrix(
      JavaPairRDD<String, List<String>> uniqueDocRDD, JavaSparkContext sc) {
    // Index documents with unique IDs
    JavaPairRDD<List<String>, Long> corpus = uniqueDocRDD.values()
        .zipWithIndex();
    // cal word-doc numbers
    JavaPairRDD<Tuple2<String, Long>, Double> worddoc_num_RDD = corpus
        .flatMapToPair(
            new PairFlatMapFunction<Tuple2<List<String>, Long>, Tuple2<String, Long>, Double>() {
              /**
               *
               */
              private static final long serialVersionUID = 1L;

              @Override
              public Iterator<Tuple2<Tuple2<String, Long>, Double>> call(
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
                return pairs.iterator();
              }
            }).reduceByKey(new Function2<Double, Double, Double>() {
          /**
           *
           */
          private static final long serialVersionUID = 1L;

          @Override
          public Double call(Double first, Double second) throws Exception {
            return first + second;
          }
        });
    // cal word doc-numbers
    JavaPairRDD<String, Tuple2<List<Long>, List<Double>>> word_docnum_RDD = worddoc_num_RDD
        .mapToPair(
            new PairFunction<Tuple2<Tuple2<String, Long>, Double>, String, Tuple2<List<Long>, List<Double>>>() {
              /**
               *
               */
              private static final long serialVersionUID = 1L;

              @Override
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
          /**
           *
           */
          private static final long serialVersionUID = 1L;

          @Override
          public Tuple2<List<Long>, List<Double>> call(
              Tuple2<List<Long>, List<Double>> arg0,
              Tuple2<List<Long>, List<Double>> arg1) throws Exception {
            arg0._1.addAll(arg1._1);
            arg0._2.addAll(arg1._2);
            return new Tuple2<List<Long>, List<Double>>(arg0._1, arg0._2);
          }
        }).mapToPair(
        new PairFunction<Tuple2<String, Tuple2<List<Long>, List<Double>>>, String, Vector>() {
          /**
           *
           */
          private static final long serialVersionUID = 1L;

          @Override
          public Tuple2<String, Vector> call(
              Tuple2<String, Tuple2<List<Long>, List<Double>>> arg0)
              throws Exception {
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

    LabeledRowMatrix labeledRowMatrix = new LabeledRowMatrix();
    labeledRowMatrix.rowMatrix = wordDocMatrix;
    labeledRowMatrix.rowkeys = word_vectorRDD.keys().collect();
    labeledRowMatrix.colkeys = uniqueDocRDD.keys().collect();
    return labeledRowMatrix;
  }

  public static LabeledRowMatrix createDocWordMatrix(
      JavaPairRDD<String, List<String>> uniqueDocRDD, JavaSparkContext sc) {
    // Index word with unique IDs
    JavaPairRDD<String, Long> wordIDRDD = uniqueDocRDD.values()
        .flatMap(new FlatMapFunction<List<String>, String>() {
          /**
           *
           */
          private static final long serialVersionUID = 1L;

          @Override
          public Iterator<String> call(List<String> arg0) throws Exception {
            return arg0.iterator();
          }
        }).distinct().zipWithIndex();

    //
    JavaPairRDD<Tuple2<String, String>, Double> docword_num_RDD = uniqueDocRDD
        .flatMapToPair(
            new PairFlatMapFunction<Tuple2<String, List<String>>, Tuple2<String, String>, Double>() {

              /**
               *
               */
              private static final long serialVersionUID = 1L;

              @Override
              public Iterator<Tuple2<Tuple2<String, String>, Double>> call(
                  Tuple2<String, List<String>> docwords) throws Exception {
                List<Tuple2<Tuple2<String, String>, Double>> pairs = new ArrayList<Tuple2<Tuple2<String, String>, Double>>();
                List<String> words = docwords._2;
                int n = words.size();
                for (int i = 0; i < n; i++) {
                  Tuple2<String, String> worddoc = new Tuple2<String, String>(
                      docwords._1, words.get(i));
                  pairs.add(
                      new Tuple2<Tuple2<String, String>, Double>(worddoc, 1.0));
                }
                return pairs.iterator();
              }
            }).reduceByKey(new Function2<Double, Double, Double>() {
          /**
           *
           */
          private static final long serialVersionUID = 1L;

          @Override
          public Double call(Double first, Double second) throws Exception {
            return first + second;
          }
        });

    //
    JavaPairRDD<String, Tuple2<String, Double>> word_docnum_RDD = docword_num_RDD
        .mapToPair(
            new PairFunction<Tuple2<Tuple2<String, String>, Double>, String, Tuple2<String, Double>>() {
              /**
               *
               */
              private static final long serialVersionUID = 1L;

              @Override
              public Tuple2<String, Tuple2<String, Double>> call(
                  Tuple2<Tuple2<String, String>, Double> arg0)
                  throws Exception {

                Tuple2<String, Double> wordmums = new Tuple2<String, Double>(
                    arg0._1._1, arg0._2);
                return new Tuple2<String, Tuple2<String, Double>>(arg0._1._2,
                    wordmums);
              }
            });

    //

    JavaPairRDD<String, Tuple2<Tuple2<String, Double>, Optional<Long>>> testRDD = word_docnum_RDD
        .leftOuterJoin(wordIDRDD);

    int wordsize = (int) wordIDRDD.count();
    JavaPairRDD<String, Vector> doc_vectorRDD = testRDD.mapToPair(
        new PairFunction<Tuple2<String, Tuple2<Tuple2<String, Double>, Optional<Long>>>, String, Tuple2<List<Long>, List<Double>>>() {
          /**
           *
           */
          private static final long serialVersionUID = 1L;

          @Override
          public Tuple2<String, Tuple2<List<Long>, List<Double>>> call(
              Tuple2<String, Tuple2<Tuple2<String, Double>, Optional<Long>>> arg0)
              throws Exception {
            Optional<Long> oid = arg0._2._2;
            Long wordId = (long) 0;
            if (oid.isPresent()) {
              wordId = oid.get();
            }

            List<Long> word = new ArrayList<Long>();
            word.add(wordId);

            List<Double> count = new ArrayList<Double>();
            count.add(arg0._2._1._2);

            Tuple2<List<Long>, List<Double>> wordcount = new Tuple2<List<Long>, List<Double>>(
                word, count);

            return new Tuple2<String, Tuple2<List<Long>, List<Double>>>(
                arg0._2._1._1, wordcount);
          }

        }).reduceByKey(
        new Function2<Tuple2<List<Long>, List<Double>>, Tuple2<List<Long>, List<Double>>, Tuple2<List<Long>, List<Double>>>() {
          /**
           *
           */
          private static final long serialVersionUID = 1L;

          @Override
          public Tuple2<List<Long>, List<Double>> call(
              Tuple2<List<Long>, List<Double>> arg0,
              Tuple2<List<Long>, List<Double>> arg1) throws Exception {
            arg0._1.addAll(arg1._1);
            arg0._2.addAll(arg1._2);
            return new Tuple2<List<Long>, List<Double>>(arg0._1, arg0._2);
          }
        }).mapToPair(
        new PairFunction<Tuple2<String, Tuple2<List<Long>, List<Double>>>, String, Vector>() {
          /**
           *
           */
          private static final long serialVersionUID = 1L;

          @Override
          public Tuple2<String, Vector> call(
              Tuple2<String, Tuple2<List<Long>, List<Double>>> arg0)
              throws Exception {
            int docsize = arg0._2._1.size();
            int[] intArray = new int[docsize];
            double[] doubleArray = new double[docsize];
            for (int i = 0; i < docsize; i++) {
              intArray[i] = arg0._2._1.get(i).intValue();
              doubleArray[i] = arg0._2._2.get(i).intValue();
            }
            Vector sv = Vectors.sparse(wordsize, intArray, doubleArray);
            return new Tuple2<String, Vector>(arg0._1, sv);
          }
        });

    RowMatrix docwordMatrix = new RowMatrix(doc_vectorRDD.values().rdd());

    LabeledRowMatrix labeledRowMatrix = new LabeledRowMatrix();
    labeledRowMatrix.rowMatrix = docwordMatrix;
    labeledRowMatrix.rowkeys = doc_vectorRDD.keys().collect();
    labeledRowMatrix.colkeys = wordIDRDD.keys().collect();

    return labeledRowMatrix;
  }

  /**
   * loadVectorFromCSV: Load term vector from csv file.
   *
   * @param spark       spark instance
   * @param csvFileName csv matrix file
   * @param skipNum     the numbers of rows which should be skipped Ignore the top skip
   *                    number rows of the csv file
   * @return JavaPairRDD, each key is a term, and value is the vector of the
   * term in feature space.
   */
  public static JavaPairRDD<String, Vector> loadVectorFromCSV(SparkDriver spark,
      String csvFileName, int skipNum) {
    // skip the first two lines(header), important!
    JavaRDD<String> importRDD = spark.sc.textFile(csvFileName);
    JavaPairRDD<String, Long> importIdRDD = importRDD.zipWithIndex()
        .filter(new Function<Tuple2<String, Long>, Boolean>() {
          /** */
          private static final long serialVersionUID = 1L;

          @Override
          public Boolean call(Tuple2<String, Long> v1) throws Exception {
            if (v1._2 > (skipNum - 1)) {
              return true;
            }
            return false;
          }
        });

    return importIdRDD
        .mapToPair(new PairFunction<Tuple2<String, Long>, String, Vector>() {
          /** */
          private static final long serialVersionUID = 1L;

          @Override
          public Tuple2<String, Vector> call(Tuple2<String, Long> t)
              throws Exception {
            String[] fields = t._1.split(",");
            String word = fields[0];
            int fieldsize = fields.length;
            String[] numfields = Arrays.copyOfRange(fields, 1, fieldsize - 1);
            double[] nums = Stream.of(numfields)
                .mapToDouble(Double::parseDouble).toArray();
            Vector vec = Vectors.dense(nums);
            return new Tuple2<>(word, vec);
          }
        });
  }

  /**
   * buildIndexRowMatrix: Converst vectorRDD to indexed row matrix.
   *
   * @param vecs Vector RDD
   * @return IndexedRowMatrix
   */
  public static IndexedRowMatrix buildIndexRowMatrix(JavaRDD<Vector> vecs) {
    JavaRDD<IndexedRow> indexrows = vecs.zipWithIndex()
        .map(new Function<Tuple2<Vector, Long>, IndexedRow>() {
          /**
           *
           */
          private static final long serialVersionUID = 1L;

          @Override
          public IndexedRow call(Tuple2<Vector, Long> docId) {
            return new IndexedRow(docId._2, docId._1);
          }
        });
    return new IndexedRowMatrix(indexrows.rdd());
  }

  /**
   * transposeMatrix:transpose matrix.
   *
   * @param indexedMatrix spark indexed matrix
   * @return rowmatrix, each row is corresponding to the column in the original
   * matrix and vice versa
   */
  public static RowMatrix transposeMatrix(IndexedRowMatrix indexedMatrix) {
    return indexedMatrix.toCoordinateMatrix().transpose().toRowMatrix();
  }

  /**
   * exportToCSV: Output matrix to a csv file.
   *
   * @param matrix   spark row matrix
   * @param rowKeys  matrix row names
   * @param colKeys  matrix coloum names
   * @param fileName csv file name
   */
  public static void exportToCSV(RowMatrix matrix, List<String> rowKeys,
      List<String> colKeys, String fileName) {

    if (matrix.rows().isEmpty()) {
      return;
    }

    int rownum = (int) matrix.numRows();
    int colnum = (int) matrix.numCols();
    List<Vector> rows = matrix.rows().toJavaRDD().collect();

    File file = new File(fileName);
    if (file.exists()) {
      file.delete();
    }
    try {
      file.createNewFile();
      FileWriter fw = new FileWriter(file.getAbsoluteFile());
      BufferedWriter bw = new BufferedWriter(fw);
      String coltitle = " Num" + ",";
      for (int j = 0; j < colnum; j++) {
        coltitle += "\"" + colKeys.get(j) + "\",";
      }
      coltitle = coltitle.substring(0, coltitle.length() - 1);
      bw.write(coltitle + "\n");

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

    } catch (IOException e) {
      e.printStackTrace();

    }
  }
}
