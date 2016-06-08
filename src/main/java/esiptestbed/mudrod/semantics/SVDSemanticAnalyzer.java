package esiptestbed.mudrod.semantics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;

import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.utils.LinkageTriple;
import esiptestbed.mudrod.utils.MatrixUtil;

public class SVDSemanticAnalyzer extends SemanticAnalyzer {

	public SVDSemanticAnalyzer(Map<String, String> config, ESDriver es, SparkDriver spark) {
		super(config, es, spark);
		// TODO Auto-generated constructor stub
	}
	
	public void GetSVDMatrix(String CSV_fileName, int svdDimention,String svd_matrix_fileName){
		
		try {
			JavaPairRDD<String, Vector> importRDD = MatrixUtil.loadVectorFromCSV(spark, CSV_fileName, 1);
			JavaRDD<Vector> vectorRDD = importRDD.values();
			RowMatrix wordDocMatrix = new RowMatrix(vectorRDD.rdd());		
			RowMatrix TFIDFMatrix = MatrixUtil.createTFIDFMatrix(wordDocMatrix, spark.sc);
			RowMatrix svdMatrix = MatrixUtil.buildSVDMatrix(TFIDFMatrix, svdDimention);
			
			List<String> rowKeys = importRDD.keys().collect();
			List<String> colKeys = new ArrayList<String>();
			for(int i=0; i<svdDimention; i++){
				colKeys.add("dimension" + i);
			}
			MatrixUtil.exportSVDMatrixToCSV(svdMatrix, rowKeys, colKeys, svd_matrix_fileName);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
