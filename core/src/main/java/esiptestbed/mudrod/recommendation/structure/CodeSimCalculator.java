package esiptestbed.mudrod.recommendation.structure;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import org.apache.commons.collections.map.LinkedMap;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.google.common.base.Optional;

import esiptestbed.mudrod.discoveryengine.MudrodAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.utils.LinkageTriple;
import esiptestbed.mudrod.utils.MatrixUtil;
import esiptestbed.mudrod.utils.SimilarityUtil;
import esiptestbed.mudrod.weblog.structure.ClickStream;
import scala.Tuple2;

public class CodeSimCalculator implements Serializable {

	private String indexName;
	private String metadataType;
	private Map<String, Double> CategoricalVarWeights;
	
	private List<Double> weights;
	private double totalWeights;
	private int termIndex;
	
	public CodeSimCalculator(Map<String, String> config) {
		// TODO Auto-generated constructor stub
		indexName = config.get("indexName");
		metadataType = config.get("recom_metadataType");
		
		OHEncoder encoder = new OHEncoder();
		CategoricalVarWeights =  encoder.CategoricalVarWeights;
		weights = new ArrayList(CategoricalVarWeights.values());
		
		int size = weights.size();
		for(int i=0; i<size; i++){
			totalWeights += weights.get(i);
		}
		
		List<String> vars = new ArrayList<String>(CategoricalVarWeights.keySet());
		for(int i=0; i<size; i++){
			if(vars.get(i).equals("DatasetParameter-Term")){
				termIndex = i;
				break;
			}
		}
	}

	public List<LinkageTriple> CalItemSimfromTxt(SparkDriver spark, String txtFile) {

		// load data
		JavaPairRDD<String, List<Vector>> importRDD = spark.sc.textFile(txtFile)
				.mapToPair(new PairFunction<String, String, List<Vector>>() {
					public Tuple2<String, List<Vector>> call(String s) {
						String[] sarray = s.split(":");
						String shortname = sarray[0];
						String code = sarray[1];

						String[] codeArr = code.split(" & ");
						int size = codeArr.length;

						List<Vector> vecs = new ArrayList<Vector>();
						for (int i = 0; i < size; i++) {

							String tmpcode = codeArr[i];
							//System.out.println(tmpcode);
							String[] values = tmpcode.split(", ");
							int valuesize = values.length;
							double[] nums = Stream.of(values).mapToDouble(Double::parseDouble).toArray();
							Vector vec = Vectors.dense(nums);
							
							vecs.add(vec);
						}
						return new Tuple2<String, List<Vector>>(shortname, vecs);
					}
				});

		
		JavaPairRDD<Tuple2<String, List<Vector>>, Tuple2<String, List<Vector>>> cartesianRDD = importRDD.cartesian(importRDD);

		JavaRDD<LinkageTriple> tripleRDD = cartesianRDD
				.map(new Function<Tuple2<Tuple2<String, List<Vector>>, Tuple2<String, List<Vector>>>, LinkageTriple>() {
					public LinkageTriple call(Tuple2<Tuple2<String, List<Vector>>, Tuple2<String, List<Vector>>> arg) {
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
						if(product == 0.0 ){
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
						// TODO Auto-generated method stub
						if (arg0 == null) {
							return false;
						}
						return true;
					}
				});

		return tripleRDD.collect();
	}

	private double similarity(List<Vector> vecListA, List<Vector> vecListB) {
		
		if(vecListA.size() != vecListB.size()){
			return 0.0;
		}
		
		int size = vecListA.size();
		double vecSum = 0.0;
		for(int i=0; i<size; i++){
			
			double sim = this.cosSimilarity(vecListA.get(i), vecListB.get(i)); 
			vecSum +=  sim * weights.get(i);
		}
		
		double totalSim = vecSum/totalWeights;

		return totalSim;
	}
	
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
		
		if(product == 0.0){
			return 0.0;
		}

		double sim = product/(Math.sqrt(tmpA) * Math.sqrt(tmpB));
		return sim;
	}
	
	public double dotProduct(Vector vecA, Vector vecB){
		double product = 0.0;
		
		double[] arrA = vecA.toDense().toArray();
		double[] arrB = vecB.toDense().toArray();
		
		int length = arrA.length;
		for(int i=0; i<length; i++){
			product += arrA[i] * arrB[i];
		}
			
		return product;
	}
}
