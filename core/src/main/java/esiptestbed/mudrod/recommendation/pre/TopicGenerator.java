/**
 * Project Name:mudrod-core
 * File Name:TopicGenerator.java
 * Package Name:esiptestbed.mudrod.recommendation.pre
 * Date:Aug 19, 20165:00:35 PM
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
 * ClassName:TopicGenerator <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason: TODO ADD REASON. <br/>
 * Date: Aug 19, 2016 5:00:35 PM <br/>
 *
 * @author Yun
 * @version
 * @since JDK 1.6
 * @see
 */
public class TopicGenerator extends DiscoveryStepAbstract {

  public TopicGenerator(Map<String, String> config, ESDriver es,
      SparkDriver spark) {

    super(config, es, spark);
    // TODO Auto-generated constructor stub

  }

  @Override
  public Object execute() {

    // TODO Auto-generated method stub

    LDAModel lda = new LDAModel(config);
    try {
      JavaPairRDD<String, List<String>> metadataVecs = lda.loadData(es, spark);
      LabeledRowMatrix dataTopicMatrix = lda.getLDA(metadataVecs, spark,
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
