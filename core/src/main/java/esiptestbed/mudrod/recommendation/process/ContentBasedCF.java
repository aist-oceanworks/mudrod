package esiptestbed.mudrod.recommendation.process;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

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

  public ContentBasedCF(Map<String, String> config, ESDriver es,
      SparkDriver spark) {
    // TODO Auto-generated constructor stub
    super(config, es, spark);
  }

  @Override
  public Object execute() {
    // TODO Auto-generated method stub
    LOG.info(
        "*****************Metadata content based similarity starts******************");
    startTime = System.currentTimeMillis();

    try {
      CodeSimCalculator calculator = new CodeSimCalculator(config);
      String MatrixCodeFileName = config.get("metadataOBCode");
      List<LinkageTriple> triples = calculator.CalItemSimfromTxt(spark,
          MatrixCodeFileName);

      SemanticAnalyzer analyzer = new SemanticAnalyzer(config, es, spark);
      analyzer.SaveToES(triples, config.get("indexName"),
          config.get("metadataCodeSimType"), true, false);

    } catch (Exception e) {
      // TODO Auto-generated catch block
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
    // TODO Auto-generated method stub
    return null;
  }

}
