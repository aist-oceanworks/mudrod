/**
 * Project Name:mudrod-core
 * File Name:TopicBasedCF.java
 * Package Name:gov.nasa.jpl.mudrod.recommendation.process
 * Date:Aug 22, 201610:45:55 AM
 * Copyright (c) 2016, chenzhou1025@126.com All Rights Reserved.
 */

package gov.nasa.jpl.mudrod.recommendation.process;

import gov.nasa.jpl.mudrod.discoveryengine.DiscoveryStepAbstract;
import gov.nasa.jpl.mudrod.driver.ESDriver;
import gov.nasa.jpl.mudrod.driver.SparkDriver;
import gov.nasa.jpl.mudrod.main.MudrodConstants;
import gov.nasa.jpl.mudrod.semantics.SVDAnalyzer;
import gov.nasa.jpl.mudrod.utils.LinkageTriple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

/**
 * ClassName: Recommend metedata based on data content semantic similarity
 */
public class AbstractBasedSimilarity extends DiscoveryStepAbstract {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractBasedSimilarity.class);

  /**
   * Creates a new instance of TopicBasedCF.
   *
   * @param props the Mudrod configuration
   * @param es    the Elasticsearch client
   * @param spark the spark drive
   */
  public AbstractBasedSimilarity(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  @Override
  public Object execute() {

    LOG.info("*****************abstract similarity calculation starts******************");
    startTime = System.currentTimeMillis();

    try {
      // for comparison
      SVDAnalyzer svd = new SVDAnalyzer(props, es, spark);
      svd.getSVDMatrix(props.getProperty(MudrodConstants.METADATA_WORD_MATRIX_PATH), 150, props.getProperty(MudrodConstants.METADATA_WORD_MATRIX_PATH));
      List<LinkageTriple> tripleList = svd.calTermSimfromMatrix(props.getProperty(MudrodConstants.METADATA_WORD_MATRIX_PATH));
      svd.saveToES(tripleList, props.getProperty(MudrodConstants.ES_INDEX_NAME), MudrodConstants.METADATA_WORD_SIM_TYPE, true, true);

    } catch (Exception e) {
      e.printStackTrace();
    }

    endTime = System.currentTimeMillis();
    LOG.info("*****************abstract similarity calculation ends******************Took {}s", (endTime - startTime) / 1000);

    return null;
  }

  @Override
  public Object execute(Object o) {
    return null;
  }
}
