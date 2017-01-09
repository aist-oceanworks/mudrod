/**
 * Project Name:mudrod-core
 * File Name:TFIDFGenerator.java
 * Package Name:esiptestbed.mudrod.recommendation.pre
 * Date:Aug 22, 201612:39:52 PM
 * Copyright (c) 2016, chenzhou1025@126.com All Rights Reserved.
 *
*/

package esiptestbed.mudrod.recommendation.pre;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.spark.api.java.JavaPairRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.recommendation.structure.MetadataOpt;
import esiptestbed.mudrod.utils.LabeledRowMatrix;
import esiptestbed.mudrod.utils.MatrixUtil;

/**
 * ClassName: Generate TFIDF information of all metadata
 */
public class MetadataTFIDFGenerator extends DiscoveryStepAbstract {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory
      .getLogger(MetadataTFIDFGenerator.class);

  /**
   * Creates a new instance of MatrixGenerator.
   *
   * @param props
   *          the Mudrod configuration
   * @param es
   *          the Elasticsearch drive
   * @param spark
   *          the spark drive
   */
  public MetadataTFIDFGenerator(Properties props, ESDriver es,
      SparkDriver spark) {
    super(props, es, spark);
  }

  @Override
  public Object execute() {

    LOG.info(
        "*****************Dataset TF_IDF Matrix Generator starts******************");

    startTime = System.currentTimeMillis();
    try {
      generateWordBasedTFIDF();
      generateTermBasedTFIDF();
    } catch (Exception e) {
      e.printStackTrace();
    }
    endTime = System.currentTimeMillis();

    LOG.info(
        "*****************Dataset TF_IDF Matrix Generator ends******************Took {}s",
        (endTime - startTime) / 1000);

    return null;
  }

  @Override
  public Object execute(Object o) {

    // TODO Auto-generated method stub
    return null;
  }

  public LabeledRowMatrix generateWordBasedTFIDF() throws Exception {

    MetadataOpt opt = new MetadataOpt(props);

    JavaPairRDD<String, String> metadataContents = opt.loadAll(es, spark);

    JavaPairRDD<String, List<String>> metadataWords = opt
        .tokenizeData(metadataContents, " ");

    LabeledRowMatrix wordtfidfMatrix = opt.TFIDFTokens(metadataWords, spark);

    MatrixUtil.exportToCSV(wordtfidfMatrix.rowMatrix, wordtfidfMatrix.rowkeys,
        wordtfidfMatrix.colkeys,
        props.getProperty("metadata_word_tfidf_matrix"));

    return wordtfidfMatrix;
  }

  public LabeledRowMatrix generateTermBasedTFIDF() throws Exception {

    MetadataOpt opt = new MetadataOpt(props);

    List<String> variables = new ArrayList<String>();
    variables.add("DatasetParameter-Term");
    variables.add("DatasetParameter-Variable");
    variables.add("Dataset-ExtractTerm");

    JavaPairRDD<String, String> metadataContents = opt.loadAll(es, spark,
        variables);

    JavaPairRDD<String, List<String>> metadataTokens = opt
        .tokenizeData(metadataContents, ",");

    LabeledRowMatrix tokentfidfMatrix = opt.TFIDFTokens(metadataTokens, spark);

    MatrixUtil.exportToCSV(tokentfidfMatrix.rowMatrix, tokentfidfMatrix.rowkeys,
        tokentfidfMatrix.colkeys,
        props.getProperty("metadata_term_tfidf_matrix"));

    return tokentfidfMatrix;
  }
}
