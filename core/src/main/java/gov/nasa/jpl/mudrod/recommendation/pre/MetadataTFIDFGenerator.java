/**
 * Project Name:mudrod-core
 * File Name:TFIDFGenerator.java
 * Package Name:gov.nasa.jpl.mudrod.recommendation.pre
 * Date:Aug 22, 201612:39:52 PM
 * Copyright (c) 2016, chenzhou1025@126.com All Rights Reserved.
 */

package gov.nasa.jpl.mudrod.recommendation.pre;

import gov.nasa.jpl.mudrod.discoveryengine.DiscoveryStepAbstract;
import gov.nasa.jpl.mudrod.driver.ESDriver;
import gov.nasa.jpl.mudrod.driver.SparkDriver;
import gov.nasa.jpl.mudrod.main.MudrodConstants;
import gov.nasa.jpl.mudrod.recommendation.structure.MetadataTokenizer;
import gov.nasa.jpl.mudrod.utils.LabeledRowMatrix;
import gov.nasa.jpl.mudrod.utils.MatrixUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * ClassName: Generate TFIDF information of all metadata
 */
public class MetadataTFIDFGenerator extends DiscoveryStepAbstract {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(MetadataTFIDFGenerator.class);

  /**
   * Creates a new instance of MatrixGenerator.
   *
   * @param props the Mudrod configuration
   * @param es    the Elasticsearch drive
   * @param spark the spark drive
   */
  public MetadataTFIDFGenerator(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  @Override
  public Object execute() {

    LOG.info("Starting Dataset TF_IDF Matrix Generator");
    startTime = System.currentTimeMillis();
    try {
      generateWordBasedTFIDF();
    } catch (Exception e) {
      LOG.error("Error during Dataset TF_IDF Matrix Generation: {}", e);
    }
    endTime = System.currentTimeMillis();

    LOG.info("Dataset TF_IDF Matrix Generation complete, time elaspsed: {}s", (endTime - startTime) / 1000);

    return null;
  }

  @Override
  public Object execute(Object o) {
    return null;
  }

  public LabeledRowMatrix generateWordBasedTFIDF() throws Exception {

    MetadataTokenizer opt = new MetadataTokenizer(props);

    String metadataName = props.getProperty(MudrodConstants.METADATA_ID);
    JavaPairRDD<String, String> metadataContents = opt.loadAll(es, spark, metadataName);

    JavaPairRDD<String, List<String>> metadataWords = opt.tokenizeData(metadataContents, " ");

    LabeledRowMatrix wordtfidfMatrix = opt.tFIDFTokens(metadataWords, spark);

    MatrixUtil.exportToCSV(wordtfidfMatrix.rowMatrix, wordtfidfMatrix.rowkeys, wordtfidfMatrix.colkeys, props.getProperty(MudrodConstants.METADATA_WORD_MATRIX_PATH));

    return wordtfidfMatrix;
  }

  public LabeledRowMatrix generateTermBasedTFIDF() throws Exception {

    MetadataTokenizer opt = new MetadataTokenizer(props);

    String source = props.getProperty(MudrodConstants.SEMANTIC_FIELDS);
    List<String> variables = new ArrayList<String>(Arrays.asList(source.split(",")));

    String metadataName = props.getProperty(MudrodConstants.METADATA_ID);
    JavaPairRDD<String, String> metadataContents = opt.loadAll(es, spark, variables, metadataName);

    JavaPairRDD<String, List<String>> metadataTokens = opt.tokenizeData(metadataContents, ",");

    LabeledRowMatrix tokentfidfMatrix = opt.tFIDFTokens(metadataTokens, spark);

    MatrixUtil.exportToCSV(tokentfidfMatrix.rowMatrix, tokentfidfMatrix.rowkeys, tokentfidfMatrix.colkeys, props.getProperty(MudrodConstants.METADATA_TERM_MATRIX_PATH));

    return tokentfidfMatrix;
  }
}
