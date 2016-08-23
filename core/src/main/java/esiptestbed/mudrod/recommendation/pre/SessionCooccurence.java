/**
 * Project Name:mudrod-core
 * File Name:SessionCooccurence.java
 * Package Name:esiptestbed.mudrod.recommendation.pre
 * Date:Aug 19, 20161:53:46 PM
 * Copyright (c) 2016, chenzhou1025@126.com All Rights Reserved.
 *
*/

package esiptestbed.mudrod.recommendation.pre;

import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.weblog.structure.ClickStream;
import esiptestbed.mudrod.weblog.structure.SessionExtractor;

/**
 * ClassName:SessionCooccurence <br/>
 * Function: TODO ADD FUNCTION. <br/>
 * Reason: TODO ADD REASON. <br/>
 * Date: Aug 19, 2016 1:53:46 PM <br/>
 *
 * @author Yun
 * @version
 * @since JDK 1.6
 * @see
 */
public class SessionCooccurence extends DiscoveryStepAbstract {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory
      .getLogger(SessionCooccurence.class);

  public SessionCooccurence(Map<String, String> config, ESDriver es,
      SparkDriver spark) {

    super(config, es, spark);
    // TODO Auto-generated constructor stub

  }

  @Override
  public Object execute() {

    LOG.info("*****************Session Cooccurence starts******************");
    startTime = System.currentTimeMillis();

    try {
      SessionExtractor extractor = new SessionExtractor();
      JavaRDD<ClickStream> clickstreamRDD = extractor
          .extractClickStreamFromES(this.config, this.es, this.spark);
      JavaPairRDD<String, Double> session_itemRDD = extractor
          .bulidSessionItermRDD(clickstreamRDD, 1);
      session_itemRDD.saveAsTextFile(config.get("session_item_opt"));

    } catch (Exception e) {
      e.printStackTrace();
    }

    endTime = System.currentTimeMillis();
    LOG.info(
        "*****************Session Cooccurence ends******************Took {}s",
        (endTime - startTime) / 1000);
    return null;
  }

  @Override
  public Object execute(Object o) {

    // TODO Auto-generated method stub
    return null;
  }

}
