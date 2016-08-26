/**
 * Project Name:mudrod-core
 * File Name:TopicBasedCF.java
 * Package Name:esiptestbed.mudrod.recommendation.process
 * Date:Aug 22, 201610:45:55 AM
 * Copyright (c) 2016, chenzhou1025@126.com All Rights Reserved.
 *
*/

package esiptestbed.mudrod.recommendation.process;

import java.util.List;
import java.util.Properties;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.semantics.SemanticAnalyzer;
import esiptestbed.mudrod.utils.LinkageTriple;

/**
 * ClassName:TopicBasedCF <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason: TODO ADD REASON. <br/>
 * Date: Aug 22, 2016 10:45:55 AM <br/>
 *
 * @author Yun
 * @version
 * @since JDK 1.6
 * @see
 */
public class TopicBasedCF extends DiscoveryStepAbstract {

  public TopicBasedCF(Properties props, ESDriver es, SparkDriver spark) {

    super(props, es, spark);
    // TODO Auto-generated constructor stub
  }

  @Override
  public Object execute() {

    try {
      String topicMatrixFile = props.getProperty("metadata_topic_matrix");
      SemanticAnalyzer analyzer = new SemanticAnalyzer(props, es, spark);
      List<LinkageTriple> triples = analyzer
          .calTermSimfromMatrix(topicMatrixFile, 1);
      analyzer.saveToES(triples, props.getProperty("indexName"),
          props.getProperty("metadataTopicSimType"), true, true);

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
