package esiptestbed.mudrod.recommendation.structure;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.weblog.structure.ClickStream;
import scala.Tuple2;

public class ModelBasedRating implements Serializable {

	public void test(ESDriver es,
		      JavaSparkContext sc, String path){
		
		JavaRDD<String> data = sc.textFile(path);
		
		JavaPairRDD<String,String> userItemRDD = data.mapToPair(new PairFunction<String, String, String>() {
	          public Tuple2<String, String> call(String s){
		      String[] sarray = s.split(",");
		      return new Tuple2<String,String>(sarray[0], sarray[1]);
	    }});
		
		JavaPairRDD<String, Long> userIDRDD = userItemRDD.keys().distinct().zipWithUniqueId();
		JavaPairRDD<String, Long> itemIDRDD = userItemRDD.values().distinct().zipWithUniqueId();
		
		JavaPairRDD<Long, String> idUserRDD = this.swapPairRDD(userIDRDD);
		JavaPairRDD<Long, String> idItem = this.swapPairRDD(itemIDRDD);
		
		/*JavaPairRDD<String, Long> idUserRDD = userIDRDD.mapToPair(new PairFunction<String, Long, String>() {
	          public Tuple2<String, String> call(String s){
		      String[] sarray = s.split(",");
		      return new Tuple2<String,String>(sarray[0], sarray[1]);
	    }});*/
		
		/*JavaRDD<Rating> ratings = data.map(
		  new Function<String, Rating>() {
		    public Rating call(String s) {
		      String[] sarray = s.split(",");
		      return new Rating(Integer.parseInt(sarray[0]), Integer.parseInt(sarray[1]),
		        Double.parseDouble(sarray[2]));
		    }
		  }
		);

		// Build the recommendation model using ALS
		int rank = 10;
		int numIterations = 10;
		MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(ratings), rank, numIterations, 0.01);

		// Evaluate the model on rating data
		JavaRDD<Tuple2<Object, Object>> userProducts = ratings.map(
		  new Function<Rating, Tuple2<Object, Object>>() {
		    public Tuple2<Object, Object> call(Rating r) {
		      return new Tuple2<Object, Object>(r.user(), r.product());
		    }
		  }
		);
		JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD.fromJavaRDD(
		  model.predict(JavaRDD.toRDD(userProducts)).toJavaRDD().map(
		    new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {
		      public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating r){
		        return new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating());
		      }
		    }
		  ));*/
		
		//System.out.println(predictions.collect());
		
	}
	
	private JavaPairRDD<Long, String> swapPairRDD(JavaPairRDD<String, Long> nameIdRDD){
		JavaPairRDD<Long,String> idUserRDD = nameIdRDD.mapToPair(new PairFunction<Tuple2<String,Long>, Long,String>(){
			@Override
			public Tuple2<Long,String> call(Tuple2<String,Long> arg0) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2<Long,String>(arg0._2, arg0._1);
			}
		});
		return idUserRDD;
	}
}
