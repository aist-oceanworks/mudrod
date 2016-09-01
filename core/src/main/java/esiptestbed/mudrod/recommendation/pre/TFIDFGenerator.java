/**
 * Project Name:mudrod-core
 * File Name:TFIDFGenerator.java
 * Package Name:esiptestbed.mudrod.recommendation.pre
 * Date:Aug 22, 201612:39:52 PM
 * Copyright (c) 2016, chenzhou1025@126.com All Rights Reserved.
 *
*/

package esiptestbed.mudrod.recommendation.pre;

import java.util.List;
import java.util.Properties;

import org.apache.spark.api.java.JavaPairRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.recommendation.structure.LDAModel;
import esiptestbed.mudrod.utils.LabeledRowMatrix;
import esiptestbed.mudrod.utils.MatrixUtil;

/**
 * ClassName:TFIDFGenerator <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason: TODO ADD REASON. <br/>
 * Date: Aug 22, 2016 12:39:52 PM <br/>
 *
 * @author Yun
 * @version
 * @since JDK 1.6
 * @see
 */
public class TFIDFGenerator extends DiscoveryStepAbstract {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory
      .getLogger(TFIDFGenerator.class);

  public TFIDFGenerator(Properties props, ESDriver es, SparkDriver spark) {

    super(props, es, spark);
    // TODO Auto-generated constructor stub

  }

  @Override
  public Object execute() {

    LOG.info(
        "*****************Dataset TF_IDF Matrix Generator starts******************");

    startTime = System.currentTimeMillis();

    LDAModel lda = new LDAModel(props);
    try {
      JavaPairRDD<String, List<String>> metadataVecs = lda.loadData(es, spark);
      LabeledRowMatrix dataTopicMatrix = lda.getTFIDF(metadataVecs, spark,
          props.getProperty("metadata_topic"));
      MatrixUtil.exportToCSV(dataTopicMatrix.wordDocMatrix,
          dataTopicMatrix.words, dataTopicMatrix.docs,
          props.getProperty("metadata_topic_matrix"));

    } catch (Exception e) {

      // TODO Auto-generated catch block
      e.printStackTrace();

    }

    LOG.info(
        "*****************Dataset TF_IDF Matrix Generator ends******************Took {}s",
        (endTime - startTime) / 1000);

    return null;
  }

  @Override
  public Object execute(Object o) {

    // TODO Auto-generated method stub
    return null;
  }

}
