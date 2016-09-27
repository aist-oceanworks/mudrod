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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.semantics.SemanticAnalyzer;
import esiptestbed.mudrod.utils.LinkageTriple;

/**
 * ClassName: Recommend metedata based on data content semantic similarity
 */
public class TopicBasedCF extends DiscoveryStepAbstract {

  private static final Logger LOG = LoggerFactory.getLogger(TopicBasedCF.class);

  /**
   * Creates a new instance of TopicBasedCF.
   *
   * @param props
   *          the Mudrod configuration
   * @param es
   *          the Elasticsearch client
   * @param spark
   *          the spark drive
   */
  public TopicBasedCF(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  @Override
  public Object execute() {

    LOG.info(
        "*****************Topic based dataset similarity calculation starts******************");
    startTime = System.currentTimeMillis();

    try {
      String topicMatrixFile = props.getProperty("metadata_topic_matrix");
      SemanticAnalyzer analyzer = new SemanticAnalyzer(props, es, spark);
      List<LinkageTriple> triples = analyzer
          .calTermSimfromMatrix(topicMatrixFile, 1);
      analyzer.saveToES(triples, props.getProperty("indexName"),
          props.getProperty("metadataTopicSimType"), true, true);

    } catch (Exception e) {
      e.printStackTrace();
    }

    endTime = System.currentTimeMillis();
    LOG.info(
        "*****************Topic based dataset similarity calculation ends******************Took {}s",
        (endTime - startTime) / 1000);

    return null;
  }

  @Override
  public Object execute(Object o) {
    return null;
  }

}
