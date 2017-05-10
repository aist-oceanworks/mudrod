/**
 * Project Name:mudrod-core
 * File Name:sessionBasedCF.java
 * Package Name:gov.nasa.jpl.mudrod.recommendation.process
 * Date:Aug 19, 20163:17:00 PM
 * Copyright (c) 2016, chenzhou1025@126.com All Rights Reserved.
 */

package gov.nasa.jpl.mudrod.recommendation.process;

import gov.nasa.jpl.mudrod.discoveryengine.DiscoveryStepAbstract;
import gov.nasa.jpl.mudrod.driver.ESDriver;
import gov.nasa.jpl.mudrod.driver.SparkDriver;
import gov.nasa.jpl.mudrod.semantics.SemanticAnalyzer;
import gov.nasa.jpl.mudrod.utils.LinkageTriple;
import gov.nasa.jpl.mudrod.utils.SimilarityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Properties;

/**
 * ClassName: Recommend metedata based on session level co-occurrence
 */
public class sessionBasedCF extends DiscoveryStepAbstract {

  private static final Logger LOG = LoggerFactory.getLogger(sessionBasedCF.class);

  /**
   * Creates a new instance of sessionBasedCF.
   *
   * @param props
   *          the Mudrod configuration
   * @param es
   *          the Elasticsearch drive
   * @param spark
   *          the spark drive
   */
  public sessionBasedCF(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  @Override
  public Object execute() {
    LOG.info("*****************Session based metadata similarity starts******************");
    startTime = System.currentTimeMillis();

    try {
      String session_metadatFile = props.getProperty("session_metadata_Matrix");
      File f = new File(session_metadatFile);
      if (f.exists()) {
        SemanticAnalyzer analyzer = new SemanticAnalyzer(props, es, spark);
        List<LinkageTriple> triples = analyzer.calTermSimfromMatrix(session_metadatFile, SimilarityUtil.SIM_PEARSON, 1);
        analyzer.saveToES(triples, props.getProperty("indexName"), props.getProperty("metadataSessionBasedSimType"), true, false);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }

    endTime = System.currentTimeMillis();
    LOG.info("*****************Session based metadata similarity ends******************Took {}s", (endTime - startTime) / 1000);

    return null;
  }

  @Override
  public Object execute(Object o) {
    return null;
  }

}
