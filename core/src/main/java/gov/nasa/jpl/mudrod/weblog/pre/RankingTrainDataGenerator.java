package gov.nasa.jpl.mudrod.weblog.pre;

import gov.nasa.jpl.mudrod.discoveryengine.DiscoveryStepAbstract;
import gov.nasa.jpl.mudrod.driver.ESDriver;
import gov.nasa.jpl.mudrod.driver.SparkDriver;
import gov.nasa.jpl.mudrod.weblog.structure.RankingTrainData;
import gov.nasa.jpl.mudrod.weblog.structure.SessionExtractor;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class RankingTrainDataGenerator extends DiscoveryStepAbstract {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(RankingTrainDataGenerator.class);

  public RankingTrainDataGenerator(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
    // TODO Auto-generated constructor stub
  }

  @Override
  public Object execute() {
    // TODO Auto-generated method stub
    LOG.info("Starting generate ranking train data.");
    startTime = System.currentTimeMillis();

    String rankingTrainFile = "E:\\Mudrod_input_data\\Testing_Data_4_1monthLog+Meta+Onto\\traing.txt";
    try {
      SessionExtractor extractor = new SessionExtractor();
      JavaRDD<RankingTrainData> rankingTrainDataRDD = extractor.extractRankingTrainData(this.props, this.es, this.spark);

      JavaRDD<String> rankingTrainData_JsonRDD = rankingTrainDataRDD.map(f -> f.toJson());

      rankingTrainData_JsonRDD.coalesce(1, true).saveAsTextFile(rankingTrainFile);

    } catch (Exception e) {
      e.printStackTrace();
    }

    endTime = System.currentTimeMillis();
    LOG.info("Ranking train data generation complete. Time elapsed {} seconds.", (endTime - startTime) / 1000);
    return null;
  }

  @Override
  public Object execute(Object o) {
    // TODO Auto-generated method stub
    return null;
  }

}
