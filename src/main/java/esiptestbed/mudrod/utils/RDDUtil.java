package esiptestbed.mudrod.utils;

import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;

public class RDDUtil {

	public RDDUtil() {
		// TODO Auto-generated constructor stub
	}
	
	public static JavaRDD<String> getAllWordsInDoc(JavaPairRDD<String, List<String>> docwordRDD){
		JavaRDD<String> wordRDD = docwordRDD.values().flatMap(new FlatMapFunction<List<String>, String>() {
			public Iterable<String> call(List<String> list) {
				return list;
			}
		}).distinct();
		
		return wordRDD;
	}
}
