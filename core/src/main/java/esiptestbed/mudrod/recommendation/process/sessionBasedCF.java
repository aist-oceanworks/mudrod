/**
 * Project Name:mudrod-core
 * File Name:sessionBasedCF.java
 * Package Name:esiptestbed.mudrod.recommendation.process
 * Date:Aug 19, 20163:17:00 PM
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
import esiptestbed.mudrod.recommendation.structure.ItemSimCalculator;
import esiptestbed.mudrod.utils.LinkageTriple;

public class sessionBasedCF extends DiscoveryStepAbstract {

  private static final Logger LOG = LoggerFactory
      .getLogger(sessionBasedCF.class);

  public sessionBasedCF(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  @Override
  public Object execute() {
    LOG.info(
        "*****************Session based metadata similarity starts******************");
    startTime = System.currentTimeMillis();

    try {
      String session_metadatFile = props.getProperty("session_item_Matrix");
      ItemSimCalculator simcal = new ItemSimCalculator(props);
      List<LinkageTriple> triples = simcal.CalItemSimfromMatrix(spark,
          session_metadatFile, 1);
      LinkageTriple.insertTriples(es, triples, props.getProperty("indexName"),
          props.getProperty("metadataSessionBasedSimType"), true, false);

    } catch (Exception e) {
      e.printStackTrace();
    }

    endTime = System.currentTimeMillis();
    LOG.info(
        "*****************Session based metadata similarity ends******************Took {}s",
        (endTime - startTime) / 1000);

    return null;
  }

  @Override
  public Object execute(Object o) {
    return null;
  }

}
