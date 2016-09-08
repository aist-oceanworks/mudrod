/**
 * Project Name:mudrod-core
 * File Name:SessionCooccurenceMatrix.java
 * Package Name:esiptestbed.mudrod.recommendation.pre
 * Date:Aug 19, 20163:06:33 PM
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
import esiptestbed.mudrod.recommendation.structure.ItemSimCalculator;
import esiptestbed.mudrod.utils.LabeledRowMatrix;
import esiptestbed.mudrod.utils.MatrixUtil;
import esiptestbed.mudrod.weblog.structure.SessionExtractor;

public class SessionCooccurenceMatrix extends DiscoveryStepAbstract {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory
      .getLogger(SessionCooccurenceMatrix.class);

  public SessionCooccurenceMatrix(Properties props, ESDriver es,
      SparkDriver spark) {
    super(props, es, spark);
  }

  @Override
  public Object execute() {

    LOG.info(
        "*****************Dataset session_based similarity Generator starts******************");

    startTime = System.currentTimeMillis();

    ItemSimCalculator simCal = new ItemSimCalculator(props);

    SessionExtractor extractor = new SessionExtractor();
    JavaPairRDD<String, List<String>> sessionDatasetRDD = extractor
        .bulidSessionItermRDD(props, es, spark);

    JavaPairRDD<String, List<String>> sessionFiltedDatasetsRDD = simCal
        .filterData(es, sessionDatasetRDD);
    LabeledRowMatrix datasetSessionMatrix = MatrixUtil
        .createWordDocMatrix(sessionFiltedDatasetsRDD, spark.sc);

    MatrixUtil.exportToCSV(datasetSessionMatrix.wordDocMatrix,
        datasetSessionMatrix.words, datasetSessionMatrix.docs,
        props.getProperty("session_item_Matrix"));

    endTime = System.currentTimeMillis();

    LOG.info(
        "*****************Dataset session_based  similarity Generator ends******************Took {}s",
        (endTime - startTime) / 1000);

    return null;
  }

  @Override
  public Object execute(Object o) {
    return null;
  }

}
