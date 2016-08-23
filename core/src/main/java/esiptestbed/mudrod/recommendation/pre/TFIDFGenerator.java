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
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;

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

  public TFIDFGenerator(Map<String, String> config, ESDriver es,
      SparkDriver spark) {

    super(config, es, spark);
    // TODO Auto-generated constructor stub

  }

  @Override
  public Object execute() {

    LDAModel lda = new LDAModel(config);
    try {
      JavaPairRDD<String, List<String>> metadataVecs = lda.loadData(es, spark);
      LabeledRowMatrix dataTopicMatrix = lda.getTFIDF(metadataVecs, spark,
          config.get("metadata_topic"));
      MatrixUtil.exportToCSV(dataTopicMatrix.wordDocMatrix,
          dataTopicMatrix.words, dataTopicMatrix.docs,
          config.get("metadata_topic_matrix"));

    } catch (Exception e) {

      // TODO Auto-generated catch block
      e.printStackTrace();

    }

    return null;
  }

  @Override
  public Object execute(Object o) {

    // TODO Auto-generated method stub
    return null;
  }

}
