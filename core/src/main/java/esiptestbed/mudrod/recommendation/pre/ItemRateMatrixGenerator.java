package esiptestbed.mudrod.recommendation.pre;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaPairRDD;
import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.recommendation.structure.ItemSimCalculator;
import esiptestbed.mudrod.utils.LabeledRowMatrix;
import esiptestbed.mudrod.utils.MatrixUtil;

public class ItemRateMatrixGenerator extends DiscoveryStepAbstract {

	public ItemRateMatrixGenerator(Map<String, String> config, ESDriver es, SparkDriver spark) {
		super(config, es, spark);
		// TODO Auto-generated constructor stub
	}

	@Override
	public Object execute() {
		System.out.println("*****************Dataset user_based similarity Generator starts******************");
		startTime = System.currentTimeMillis();

		try {
			ItemSimCalculator simCal = new ItemSimCalculator(config);
			JavaPairRDD<String, List<String>> userDatasetRDD= simCal.prepareData(spark.sc, config.get("user_item_rate"));
			
			JavaPairRDD<String, List<String>> filterUserDatasetsRDD = simCal.filterData(es,userDatasetRDD);
			LabeledRowMatrix wordDocMatrix = MatrixUtil.createWordDocMatrix(filterUserDatasetsRDD, spark.sc);

			MatrixUtil.exportToCSV(wordDocMatrix.wordDocMatrix, wordDocMatrix.words, wordDocMatrix.docs,
			          config.get("user_based_item_optMatrix"));
			      
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		endTime = System.currentTimeMillis();
		System.out.println("*****************Dataset user_based  similarity Generator ends******************");
		return null;
	}

	@Override
	public Object execute(Object o) {
		// TODO Auto-generated method stub
		return null;
	}
}
