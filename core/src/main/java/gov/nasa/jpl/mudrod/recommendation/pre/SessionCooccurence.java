/**
 * Project Name:mudrod-core
 * File Name:SessionCooccurenceMatrix.java
 * Package Name:gov.nasa.jpl.mudrod.recommendation.pre
 * Date:Aug 19, 20163:06:33 PM
 * Copyright (c) 2016, chenzhou1025@126.com All Rights Reserved.
 */

package gov.nasa.jpl.mudrod.recommendation.pre;

import gov.nasa.jpl.mudrod.discoveryengine.DiscoveryStepAbstract;
import gov.nasa.jpl.mudrod.driver.ESDriver;
import gov.nasa.jpl.mudrod.driver.SparkDriver;
import gov.nasa.jpl.mudrod.main.MudrodConstants;
import gov.nasa.jpl.mudrod.utils.LabeledRowMatrix;
import gov.nasa.jpl.mudrod.utils.MatrixUtil;
import gov.nasa.jpl.mudrod.weblog.structure.SessionExtractor;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;

/**
 * ClassName: SessionCooccurenceMatrix Function: Generate metadata session
 * coocucurence matrix from web logs. Each row in the matrix is corresponding to
 * a metadata, and each column is a session.
 */
public class SessionCooccurence extends DiscoveryStepAbstract {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(SessionCooccurence.class);

  /**
   * Creates a new instance of SessionCooccurence.
   *
   * @param props
   *          the Mudrod configuration
   * @param es
   *          the Elasticsearch drive
   * @param spark
   *          the spark driver
   */
  public SessionCooccurence(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  @Override
  public Object execute() {

    LOG.info("Starting dataset session-based similarity generation...");

    startTime = System.currentTimeMillis();

    // get all metadata session cooccurance data
    SessionExtractor extractor = new SessionExtractor();
    JavaPairRDD<String, List<String>> sessionDatasetRDD = extractor.bulidSessionDatasetRDD(props, es, spark);

    // remove retired datasets
    JavaPairRDD<String, List<String>> sessionFiltedDatasetsRDD = removeRetiredDataset(es, sessionDatasetRDD);
    LabeledRowMatrix datasetSessionMatrix = MatrixUtil.createWordDocMatrix(sessionFiltedDatasetsRDD);

    // export
    MatrixUtil.exportToCSV(datasetSessionMatrix.rowMatrix, datasetSessionMatrix.rowkeys, datasetSessionMatrix.colkeys, props.getProperty("session_metadata_Matrix"));

    endTime = System.currentTimeMillis();

    LOG.info("Completed dataset session-based  similarity generation. Time elapsed: {}s", (endTime - startTime) / 1000);

    return null;
  }

  @Override
  public Object execute(Object o) {
    return null;
  }

  /**
   * filter out-of-data metadata
   *
   * @param es
   *          the Elasticsearch drive
   * @param userDatasetsRDD
   *          dataset extracted from session
   * @return filtered session datasets
   */
  public JavaPairRDD<String, List<String>> removeRetiredDataset(ESDriver es, JavaPairRDD<String, List<String>> userDatasetsRDD) {

    Map<String, String> nameMap = this.getOnServiceMetadata(es);

    return userDatasetsRDD.mapToPair(new PairFunction<Tuple2<String, List<String>>, String, List<String>>() {
      /**
       * 
       */
      private static final long serialVersionUID = 1L;

      @Override
      public Tuple2<String, List<String>> call(Tuple2<String, List<String>> arg0) throws Exception {
        List<String> oriDatasets = arg0._2;
        List<String> newDatasets = new ArrayList<>();
        int size = oriDatasets.size();
        for (int i = 0; i < size; i++) {
          String name = oriDatasets.get(i);
          if (nameMap.containsKey(name)) {
            newDatasets.add(nameMap.get(name));
          }
        }
        return new Tuple2<>(arg0._1, newDatasets);
      }
    });

  }

  /**
   * getMetadataNameMap: Get on service metadata names, key is lowcase of short
   * name and value is the original short name
   *
   * @param es
   *          the elasticsearch client
   * @return a map from lower case metadata name to original metadata name
   */
  private Map<String, String> getOnServiceMetadata(ESDriver es) {

    String indexName = props.getProperty(MudrodConstants.ES_INDEX_NAME);
    String metadataType = props.getProperty("recom_metadataType");

    Map<String, String> shortnameMap = new HashMap<>();
    SearchResponse scrollResp = es.getClient().prepareSearch(indexName).setTypes(metadataType).setScroll(new TimeValue(60000)).setQuery(QueryBuilders.matchAllQuery()).setSize(100).execute()
        .actionGet();
    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> metadata = hit.getSource();
        String shortName = (String) metadata.get("Dataset-ShortName");
        shortnameMap.put(shortName.toLowerCase(), shortName);
      }

      scrollResp = es.getClient().prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }
    }

    return shortnameMap;
  }

}
