package esiptestbed.mudrod.recommendation.process;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.recommendation.structure.CodeSimCalculator;
import esiptestbed.mudrod.semantics.SemanticAnalyzer;
import esiptestbed.mudrod.utils.LinkageTriple;

public class ContentBasedCF extends DiscoveryStepAbstract
    implements Serializable {

  private static final Logger LOG = LoggerFactory
      .getLogger(ContentBasedCF.class);

  public ContentBasedCF(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  @Override
  public Object execute() {
    LOG.info(
        "*****************Metadata content based similarity starts******************");
    startTime = System.currentTimeMillis();

    try {
      CodeSimCalculator calculator = new CodeSimCalculator(props);
      String MatrixCodeFileName = props.getProperty("metadataOBCode");
      List<LinkageTriple> triples = calculator.CalItemSimfromTxt(spark,
          MatrixCodeFileName);

      SemanticAnalyzer analyzer = new SemanticAnalyzer(props, es, spark);
      analyzer.saveToES(triples, props.getProperty("indexName"),
          props.getProperty("metadataCodeSimType"), true, false);

    } catch (Exception e) {
      e.printStackTrace();
    }

    endTime = System.currentTimeMillis();
    LOG.info(
        "*****************Metadata content based similarity ends******************Took {}s",
        (endTime - startTime) / 1000);

    return null;
  }

  @Override
  public Object execute(Object o) {
    return null;
  }

}
