package esiptestbed.mudrod.recommendation.structure;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.map.LinkedMap;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import com.google.common.base.Optional;

import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.weblog.structure.ClickStream;
import scala.Tuple2;

public class ItemSimCalculator implements Serializable {
	
	JavaPairRDD<String, Long> userIDRDD = null;
	JavaPairRDD<String, Long> itemIDRDD = null;
	MatrixFactorizationModel model = null;

	public JavaPairRDD<String, List<String>> prepareData(JavaSparkContext sc, String path) {

		// load data
		JavaRDD<String> data = sc.textFile(path).map(new Function<String, String>() {
			public String call(String s) {
				String line = s.substring(1, s.length() - 1);
				return line;
			}
		});

		// generate user-id, item-id
		JavaPairRDD<String, Integer> useritem_rateRDD = data.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) {
				String[] sarray = s.split(",");
				Double rate = Double.parseDouble(sarray[2]);
				return new Tuple2<String, Integer>(sarray[0] + "," + sarray[1], rate.intValue());
			}
		});
		
		
	    JavaPairRDD<String, List<String>> testRDD = useritem_rateRDD.flatMapToPair(
	            new PairFlatMapFunction<Tuple2<String, Integer>, String, List<String>>() {
					@Override
					public Iterable<Tuple2<String, List<String>>> call(Tuple2<String, Integer> arg0) throws Exception {
						// TODO Auto-generated method stub
						List<Tuple2<String, List<String>>> pairs = new ArrayList<Tuple2<String, List<String>>>();
		                  String words = arg0._1;
		                  int n = arg0._2;
		                  for (int i = 0; i < n; i++) {
		                	List<String> l = new ArrayList<String>();
		                	l.add(words.split(",")[1]);
		                    Tuple2<String, List<String>> worddoc = new Tuple2<String, List<String>>(
		                    		words.split(",")[0], l);
		                    pairs.add(worddoc);
		                  }
		                  return pairs;
					}
	              })
	          .reduceByKey(new Function2<List<String>, List<String>, List<String>>() {
				@Override
				public List<String> call(List<String> arg0, List<String> arg1) throws Exception {
					// TODO Auto-generated method stub
					List<String> newlist = new ArrayList<String>();
					newlist.addAll(arg0);
					newlist.addAll(arg1);
					return newlist;
				}
	          });
	    

	    return testRDD;
	}
}
