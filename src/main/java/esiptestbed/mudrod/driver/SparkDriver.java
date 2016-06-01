package esiptestbed.mudrod.driver;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkDriver implements Serializable {
	public JavaSparkContext sc;
	
	public SparkDriver() {
		SparkConf conf = new SparkConf().setAppName("Testing").setMaster("local[2]");
		sc = new JavaSparkContext(conf);
	}
}
