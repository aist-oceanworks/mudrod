package esiptestbed.mudrod.recommendation.structure;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.map.LinkedMap;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import com.google.common.base.Optional;

import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.weblog.structure.ClickStream;
import scala.Tuple2;

public class ModelBasedRating implements Serializable {
	
	JavaPairRDD<String, Long> userIDRDD = null;
	JavaPairRDD<String, Long> itemIDRDD = null;
	MatrixFactorizationModel model = null;
	
	public void predictRating(ESDriver es, JavaSparkContext sc, String path) throws IOException {

		//prepare training data
		JavaRDD<String> userid_itemid_rateRDD = this.prepareTrainData(sc, path);
		// Build the recommendation model using ALS
		JavaRDD<Rating> ratings = userid_itemid_rateRDD.map(new Function<String, Rating>() {
			public Rating call(String s) {
				String[] sarray = s.split(",");
				return new Rating(Integer.parseInt(sarray[0]), Integer.parseInt(sarray[1]),
						Double.parseDouble(sarray[2]));
			}
		});
		int rank = 10;
		int numIterations = 10;
		this.model = ALS.train(JavaRDD.toRDD(ratings), rank, numIterations, 0.01);
	}
	
	private JavaRDD<String> prepareTrainData(JavaSparkContext sc, String path) {

		// load data
		JavaRDD<String> data = sc.textFile(path).map(new Function<String, String>() {
			public String call(String s) {
				String line = s.substring(1, s.length() - 1);
				return line;
			}
		});

		// generate user-id, item-id
		JavaPairRDD<String, String> userItemRDD = data.mapToPair(new PairFunction<String, String, String>() {
			public Tuple2<String, String> call(String s) {
				String[] sarray = s.split(",");
				return new Tuple2<String, String>(sarray[0], sarray[1]);
			}
		});
		
		this.userIDRDD = userItemRDD.keys().distinct().zipWithIndex();
		this.itemIDRDD = userItemRDD.values().distinct().zipWithIndex();

		JavaPairRDD<String, String> user_itemrateRDD = data.mapToPair(new PairFunction<String, String, String>() {
			public Tuple2<String, String> call(String s) {
				String[] sarray = s.split(",");
				return new Tuple2<String, String>(sarray[0], sarray[1] + "," + sarray[2]);
			}
		});

		JavaPairRDD<String, String> item_useridrateRDD = user_itemrateRDD.leftOuterJoin(userIDRDD).values()
				.mapToPair(new PairFunction<Tuple2<String, Optional<Long>>, String, String>() {
					@Override
					public Tuple2<String, String> call(Tuple2<String, Optional<Long>> arg0) throws Exception {
						// TODO Auto-generated method stub
						Long id = (long) 0;
						Optional<Long> oid = arg0._2;
						if (oid.isPresent()) {
							id = oid.get();
						}

						String[] sarray = arg0._1.split(",");
						String userid_rate = id + "," + sarray[1];
						return new Tuple2<String, String>(sarray[0], userid_rate);
					}
				});

		JavaRDD<String> userid_itemid_rateRDD = item_useridrateRDD.leftOuterJoin(itemIDRDD).values()
				.map(new Function<Tuple2<String, Optional<Long>>, String>() {
					@Override
					public String call(Tuple2<String, Optional<Long>> arg0) throws Exception {
						// TODO Auto-generated method stub
						Long id = (long) 0;
						Optional<Long> oid = arg0._2;
						if (oid.isPresent()) {
							id = oid.get();
						}

						String[] sarray = arg0._1.split(",");
						String userid_itemid_rate = sarray[0] + "," + id + "," + sarray[1];
						return userid_itemid_rate;
					}
				});

		return userid_itemid_rateRDD;
	}

	public void saveToCSV(String fileName) throws IOException{

		File file = new File(fileName);
		if (file.exists()) {
			file.delete();
		}
		file.createNewFile();
		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);

		//col name
		List<Tuple2<String, Long>> colKeys = userIDRDD.collect();
		if (colKeys != null) {
			String coltitle = " Num" + ",";
			int size = colKeys.size();
			for (int i=0; i<size; i++) {
				coltitle += colKeys.get(i)._1 + ",";
			}
			coltitle = coltitle.substring(0, coltitle.length() - 1);
			bw.write(coltitle + "\n");
		}
		
		//row name
		List<Tuple2<String, Long>> rowKeys = itemIDRDD.collect();
		Map<Long, String> id_itemMap = new HashMap<Long, String>();
		for (int i = 0; i < rowKeys.size(); i++) {
			id_itemMap.put(rowKeys.get(i)._2, rowKeys.get(i)._1);
		}
		
		//predict row values
		int rownum = (int) itemIDRDD.count();
		int colnum = (int) userIDRDD.count();
		for (int i = 0; i < rownum; i++) {
			int itemID = i;
			JavaRDD<Tuple2<Object, Object>> userProducts = userIDRDD.values()
					.map(new Function<Long, Tuple2<Object, Object>>() {
						@Override
						public Tuple2<Object, Object> call(Long arg0) throws Exception {
							// TODO Auto-generated method stub
							return new Tuple2<Object, Object>(arg0.intValue(), itemID);
						}
					});

			JavaPairRDD<Integer, Double> predictions = JavaPairRDD
					.fromJavaRDD(model.predict(JavaRDD.toRDD(userProducts)).toJavaRDD()
							.map(new Function<Rating, Tuple2<Integer, Double>>() {
								public Tuple2<Integer, Double> call(Rating r) {
									return new Tuple2<>(r.user(), r.rating());
								}
							})).sortByKey();

			List<Double> rates = predictions.values().collect();

			String row = id_itemMap.get(Long.valueOf(i)) + ",";
			for (int j = 0; j < colnum; j++) {
				row += rates.get(j) + ",";
			}
			row = row.substring(0, row.length() - 1);
			bw.write(row + "\n");
		}

		bw.close();
	}

	/*public void predictRating(ESDriver es, JavaSparkContext sc, String path) throws IOException {
		//load data
		JavaRDD<String> data = sc.textFile(path).map(new Function<String, String>() {
			public String call(String s) {
				String line = s.substring(1, s.length() - 1);
				return line;
			}
		});
		
		//generate user-id, item-id
		JavaPairRDD<String, String> userItemRDD = data.mapToPair(new PairFunction<String, String, String>() {
			public Tuple2<String, String> call(String s) {
				String[] sarray = s.split(",");
				return new Tuple2<String, String>(sarray[0], sarray[1]);
			}
		});
		JavaPairRDD<String, Long> userIDRDD = userItemRDD.keys().distinct().zipWithIndex();
		JavaPairRDD<String, Long> itemIDRDD = userItemRDD.values().distinct().zipWithIndex();

		// Build the recommendation model using ALS
		JavaRDD<String> userid_itemid_rateRDD = this.prepareTrainData(data, userIDRDD, itemIDRDD);
		JavaRDD<Rating> ratings = userid_itemid_rateRDD.map(new Function<String, Rating>() {
			public Rating call(String s) {
				String[] sarray = s.split(",");
				return new Rating(Integer.parseInt(sarray[0]), Integer.parseInt(sarray[1]),
						Double.parseDouble(sarray[2]));
			}
		});
		int rank = 10;
		int numIterations = 10;
		MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(ratings), rank, numIterations, 0.01);
		
		//save prediction to csv file
		String fileName = "E:/podaaclogs/test.csv";
		this.exportToCSV(userIDRDD, itemIDRDD, model, fileName);
	}
	
	private JavaRDD<String> prepareTrainData(JavaRDD<String> data, JavaPairRDD<String, Long> userIDRDD, JavaPairRDD<String, Long> itemIDRDD){
		
		JavaPairRDD<String, String> user_itemrateRDD = data.mapToPair(new PairFunction<String, String, String>() {
			public Tuple2<String, String> call(String s) {
				String[] sarray = s.split(",");
				return new Tuple2<String, String>(sarray[0], sarray[1] + "," + sarray[2]);
			}
		});

		JavaPairRDD<String, String> item_useridrateRDD = user_itemrateRDD.leftOuterJoin(userIDRDD).values()
				.mapToPair(new PairFunction<Tuple2<String, Optional<Long>>, String, String>() {
					@Override
					public Tuple2<String, String> call(Tuple2<String, Optional<Long>> arg0) throws Exception {
						// TODO Auto-generated method stub
						Long id = (long) 0;
						Optional<Long> oid = arg0._2;
						if (oid.isPresent()) {
							id = oid.get();
						}

						String[] sarray = arg0._1.split(",");
						String userid_rate = id + "," + sarray[1];
						return new Tuple2<String, String>(sarray[0], userid_rate);
					}
				});

		JavaRDD<String> userid_itemid_rateRDD = item_useridrateRDD.leftOuterJoin(itemIDRDD).values()
				.map(new Function<Tuple2<String, Optional<Long>>, String>() {
					@Override
					public String call(Tuple2<String, Optional<Long>> arg0) throws Exception {
						// TODO Auto-generated method stub
						Long id = (long) 0;
						Optional<Long> oid = arg0._2;
						if (oid.isPresent()) {
							id = oid.get();
						}

						String[] sarray = arg0._1.split(",");
						String userid_itemid_rate = sarray[0] + "," + id + "," + sarray[1];
						return userid_itemid_rate;
					}
				});
		
		return userid_itemid_rateRDD;
	}

	public void exportToCSV(JavaPairRDD<String, Long> userIDRDD, JavaPairRDD<String, Long> itemIDRDD, MatrixFactorizationModel model, String fileName) throws IOException{

		File file = new File(fileName);
		if (file.exists()) {
			file.delete();
		}
		file.createNewFile();
		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);

		//col name
		List<Tuple2<String, Long>> colKeys = userIDRDD.collect();
		if (colKeys != null) {
			String coltitle = " Num" + ",";
			int size = colKeys.size();
			for (int i=0; i<size; i++) {
				coltitle += colKeys.get(i)._1 + ",";
			}
			coltitle = coltitle.substring(0, coltitle.length() - 1);
			bw.write(coltitle + "\n");
		}
		
		//row name
		List<Tuple2<String, Long>> rowKeys = itemIDRDD.collect();
		Map<Long, String> id_itemMap = new HashMap<Long, String>();
		for (int i = 0; i < rowKeys.size(); i++) {
			id_itemMap.put(rowKeys.get(i)._2, rowKeys.get(i)._1);
		}
		
		//predict row values
		int rownum = (int) itemIDRDD.count();
		int colnum = (int) userIDRDD.count();
		for (int i = 0; i < rownum; i++) {
			int itemID = i;
			JavaRDD<Tuple2<Object, Object>> userProducts = userIDRDD.values()
					.map(new Function<Long, Tuple2<Object, Object>>() {
						@Override
						public Tuple2<Object, Object> call(Long arg0) throws Exception {
							// TODO Auto-generated method stub
							return new Tuple2<Object, Object>(arg0.intValue(), itemID);
						}
					});

			JavaPairRDD<Integer, Double> predictions = JavaPairRDD
					.fromJavaRDD(model.predict(JavaRDD.toRDD(userProducts)).toJavaRDD()
							.map(new Function<Rating, Tuple2<Integer, Double>>() {
								public Tuple2<Integer, Double> call(Rating r) {
									return new Tuple2<>(r.user(), r.rating());
								}
							})).sortByKey();

			List<Double> rates = predictions.values().collect();

			String row = id_itemMap.get(Long.valueOf(i)) + ",";
			for (int j = 0; j < colnum; j++) {
				row += rates.get(j) + ",";
			}
			row = row.substring(0, row.length() - 1);
			bw.write(row + "\n");
		}

		bw.close();
	}*/
}
