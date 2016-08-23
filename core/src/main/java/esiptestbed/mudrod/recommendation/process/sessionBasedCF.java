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
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.recommendation.structure.ItemSimCalculator;
import esiptestbed.mudrod.utils.LinkageTriple;

/**
 * ClassName:sessionBasedCF <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason: TODO ADD REASON. <br/>
 * Date: Aug 19, 2016 3:17:00 PM <br/>
 *
 * @author Yun
 * @version
 * @since JDK 1.6
 * @see
 */
public class sessionBasedCF extends DiscoveryStepAbstract {

  private static final Logger LOG = LoggerFactory
      .getLogger(sessionBasedCF.class);

  public sessionBasedCF(Map<String, String> config, ESDriver es,
      SparkDriver spark) {

    super(config, es, spark);
    // TODO Auto-generated constructor stub

  }

  @Override
  public Object execute() {
    LOG.info(
        "*****************Session based metadata similarity starts******************");
    startTime = System.currentTimeMillis();

    // TODO Auto-generated method stub
    try {
      String session_metadatFile = config.get("session_item_Matrix");
      ItemSimCalculator simcal = new ItemSimCalculator(config);
      List<LinkageTriple> triples = simcal.CalItemSimfromMatrix(spark,
          session_metadatFile, 1);
      LinkageTriple.insertTriples(es, triples, config.get("indexName"),
          config.get("metadataSessionBasedSimType"), true, false);

    } catch (Exception e) {
      // TODO Auto-generated catch block
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

    // TODO Auto-generated method stub
    return null;
  }

}
