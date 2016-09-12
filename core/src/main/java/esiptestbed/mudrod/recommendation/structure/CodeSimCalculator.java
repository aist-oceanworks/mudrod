package esiptestbed.mudrod.recommendation.structure;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.utils.LinkageTriple;
import scala.Tuple2;

/**
 * Calculate metadata code similarity
 */
public class CodeSimCalculator implements Serializable {

  // index name of mudrod
  private String indexName;
  // metadata type name in ES
  private String metadataType;
  // weight of metada variables for calculating similarity
  private Map<String, Double> CategoricalVarWeights;
  // weight list
  private List<Double> weights;
  // sum of all weights
  private double totalWeights;
  // index of term
  private int termIndex;

  /**
   * Creates a new instance of CodeSimCalculator.
   *
   * @param props
   *          the Mudrod configuration
   */
  public CodeSimCalculator(Properties props) {
    // TODO Auto-generated constructor stub
    indexName = props.getProperty("indexName");
    metadataType = props.getProperty("recom_metadataType");

    OHEncoder encoder = new OHEncoder();
    CategoricalVarWeights = encoder.CategoricalVarWeights;
    weights = new ArrayList(CategoricalVarWeights.values());

    int size = weights.size();
    for (int i = 0; i < size; i++) {
      totalWeights += weights.get(i);
    }

    List<String> vars = new ArrayList<String>(CategoricalVarWeights.keySet());
    for (int i = 0; i < size; i++) {
      if (vars.get(i).equals("DatasetParameter-Term")) {
        termIndex = i;
        break;
      }
    }
  }

  /**
   * CalItemSimfromTxt:Calculate metadata similarity based on variable code
   *
   * @param spark
   *          spark client
   * @param txtFile
   *          metadta code file
   * @return triple list, each triple is according to similarity between two
   *         metadata
   *
   */
  public List<LinkageTriple> CalItemSimfromTxt(SparkDriver spark,
      String txtFile) {

    // load data
    JavaPairRDD<String, List<Vector>> importRDD = spark.sc.textFile(txtFile)
        .mapToPair(new PairFunction<String, String, List<Vector>>() {
          @Override
          public Tuple2<String, List<Vector>> call(String s) {
            String[] sarray = s.split(":");
            String shortname = sarray[0];
            String code = sarray[1];

            String[] codeArr = code.split(" & ");
            int size = codeArr.length;

            List<Vector> vecs = new ArrayList<Vector>();
            for (int i = 0; i < size; i++) {

              String tmpcode = codeArr[i];
              // System.out.println(tmpcode);
              String[] values = tmpcode.split(", ");
              int valuesize = values.length;
              double[] nums = Stream.of(values).mapToDouble(Double::parseDouble)
                  .toArray();
              Vector vec = Vectors.dense(nums);

              vecs.add(vec);
            }
            return new Tuple2<String, List<Vector>>(shortname, vecs);
          }
        });

    JavaPairRDD<Tuple2<String, List<Vector>>, Tuple2<String, List<Vector>>> cartesianRDD = importRDD
        .cartesian(importRDD);

    JavaRDD<LinkageTriple> tripleRDD = cartesianRDD.map(
        new Function<Tuple2<Tuple2<String, List<Vector>>, Tuple2<String, List<Vector>>>, LinkageTriple>() {
          @Override
          public LinkageTriple call(
              Tuple2<Tuple2<String, List<Vector>>, Tuple2<String, List<Vector>>> arg) {
            String keyA = arg._1._1;
            String keyB = arg._2._1;
            if (keyA.equals(keyB)) {
              return null;
            }

            List<Vector> vecA = arg._1._2;
            List<Vector> vecB = arg._2._2;

            Vector termVecA = vecA.get(termIndex);
            Vector termVecB = vecB.get(termIndex);

            double product = dotProduct(termVecA, termVecB);
            if (product == 0.0) {
              return null;
            }

            Double weight = similarity(vecA, vecB);
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

    return tripleRDD.collect();
  }

  /**
   * similarity:calculate similarity of two list of vectors
   *
   * @param vecListA
   *          vector list
   * @param vecListB
   *          vector list
   * @return cosine similarity
   */
  private double similarity(List<Vector> vecListA, List<Vector> vecListB) {
    if (vecListA.size() != vecListB.size()) {
      return 0.0;
    }

    int size = vecListA.size();
    double vecSum = 0.0;
    for (int i = 0; i < size; i++) {

      double sim = this.cosSimilarity(vecListA.get(i), vecListB.get(i));
      vecSum += sim * weights.get(i);
    }

    double totalSim = vecSum / totalWeights;
    return totalSim;
  }

  /**
   * cosSimilarity:calculate cosine similarity of two vectors
   *
   * @param vecA
   *          one vector
   * @param vecB
   *          one vector
   * @return cosine similarity
   */
  private double cosSimilarity(Vector vecA, Vector vecB) {
    double product = 0.0;

    double[] arrA = vecA.toDense().toArray();
    double[] arrB = vecB.toDense().toArray();

    int length = arrA.length;

    double tmpA = 0.0;
    double tmpB = 0.0;
    for (int i = 0; i < length; i++) {
      product += arrA[i] * arrB[i];

      tmpA += arrA[i] * arrA[i];
      tmpB += arrB[i] * arrB[i];
    }

    if (product == 0.0) {
      return 0.0;
    }

    double sim = product / (Math.sqrt(tmpA) * Math.sqrt(tmpB));
    return sim;
  }

  /**
   * dotProduct:calculate dot product of two vectors
   *
   * @param vecA
   *          one vector
   * @param vecB
   *          one vector
   * @return dot product
   */
  public double dotProduct(Vector vecA, Vector vecB) {
    double product = 0.0;

    double[] arrA = vecA.toDense().toArray();
    double[] arrB = vecB.toDense().toArray();

    int length = arrA.length;
    for (int i = 0; i < length; i++) {
      product += arrA[i] * arrB[i];
    }

    return product;
  }
}
