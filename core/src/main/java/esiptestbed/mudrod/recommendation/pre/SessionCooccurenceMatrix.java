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
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.recommendation.structure.ItemSimCalculator;
import esiptestbed.mudrod.utils.LabeledRowMatrix;
import esiptestbed.mudrod.utils.MatrixUtil;
import esiptestbed.mudrod.weblog.structure.SessionExtractor;

/**
 * ClassName:SessionCooccurenceMatrix <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason: TODO ADD REASON. <br/>
 * Date: Aug 19, 2016 3:06:33 PM <br/>
 *
 * @author Yun
 * @version
 * @since JDK 1.6
 * @see
 */
public class SessionCooccurenceMatrix extends DiscoveryStepAbstract {

  public SessionCooccurenceMatrix(Map<String, String> config, ESDriver es,
      SparkDriver spark) {

    super(config, es, spark);
    // TODO Auto-generated constructor stub

  }

  @Override
  public Object execute() {

    System.out.println(
        "*****************Dataset session_based similarity Generator starts******************");
    startTime = System.currentTimeMillis();

    ItemSimCalculator simCal = new ItemSimCalculator(config);

    SessionExtractor extractor = new SessionExtractor();
    JavaPairRDD<String, List<String>> sessionDatasetRDD = extractor
        .bulidSessionItermRDD(config, es, spark);

    JavaPairRDD<String, List<String>> sessionFiltedDatasetsRDD = simCal
        .filterData(es, sessionDatasetRDD);
    LabeledRowMatrix datasetSessionMatrix = MatrixUtil
        .createWordDocMatrix(sessionFiltedDatasetsRDD, spark.sc);

    MatrixUtil.exportToCSV(datasetSessionMatrix.wordDocMatrix,
        datasetSessionMatrix.words, datasetSessionMatrix.docs,
        config.get("session_item_Matrix"));

    endTime = System.currentTimeMillis();
    System.out.println(
        "*****************Dataset session_based  similarity Generator ends******************");

    return null;
  }

  @Override
  public Object execute(Object o) {

    // TODO Auto-generated method stub
    return null;
  }

}
