package esiptestbed.mudrod.discoveryengine;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.recommendation.pre.ProcessMetadataVariables;
import esiptestbed.mudrod.recommendation.process.VariableBasedSimilarity;

public class RecommendEngine extends DiscoveryEngineAbstract {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory
      .getLogger(RecommendEngine.class);

  public RecommendEngine(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
    // TODO Auto-generated constructor stub
    LOG.info("Started Mudrod Recommend Engine.");
  }

  @Override
  public void preprocess() {
    // TODO Auto-generated method stub
    LOG.info(
        "*****************Recommendation preprocessing starts******************");

    startTime = System.currentTimeMillis();

    /* DiscoveryStepAbstract harvester = new ImportMetadata(this.props, this.es,
        this.spark);
    harvester.execute();*/

    /* DiscoveryStepAbstract extractor = new ExtractMetadataTerms(this.props,
        this.es, this.spark);
    extractor.execute();
    
    DiscoveryStepAbstract tfidf = new TFIDFGenerator(this.props, this.es,
        this.spark);
    tfidf.execute();*/

    /*DiscoveryStepAbstract sessionMatrixGen = new SessionCooccurence(this.props,
        this.es, this.spark);
    sessionMatrixGen.execute();*/

    DiscoveryStepAbstract transformer = new ProcessMetadataVariables(this.props,
        this.es, this.spark);
    transformer.execute();

    /*   DiscoveryStepAbstract transformer = new ProcessMetadataVariables(this.props,
        this.es, this.spark);
    transformer.execute();
    
    DiscoveryStepAbstract obencoder = new OHEncodeMetadata(this.props, this.es,
        this.spark);
    obencoder.execute();
    
    DiscoveryStepAbstract matrixGen = new OHCodeMatrixGenerator(this.props,
        this.es, this.spark);
    matrixGen.execute();
    
    
    */

    endTime = System.currentTimeMillis();

    LOG.info(
        "*****************Recommendation preprocessing  ends******************Took {}s {}",
        (endTime - startTime) / 1000);
  }

  @Override
  public void process() {
    // TODO Auto-generated method stub
    LOG.info(
        "*****************Recommendation processing starts******************");

    startTime = System.currentTimeMillis();

    /* DiscoveryStepAbstract tfCF = new TFIDFBasedCF(this.props, this.es,
        this.spark);
    tfCF.execute();*/

    /* DiscoveryStepAbstract sbCF = new sessionBasedCF(this.props, this.es,
        this.spark);
    sbCF.execute();*/

    DiscoveryStepAbstract cbCF = new VariableBasedSimilarity(this.props,
        this.es, this.spark);
    cbCF.execute();

    /*DiscoveryStepAbstract cbCF = new ContentBasedCF(this.props, this.es,
        this.spark);
    cbCF.execute();*/

    /*DiscoveryStepAbstract tbCF = new TopicBasedCF(this.props, this.es,
        this.spark);
    tbCF.execute();*/

    endTime = System.currentTimeMillis();

    LOG.info(
        "*****************Recommendation processing ends******************Took {}s {}",
        (endTime - startTime) / 1000);
  }

  @Override
  public void output() {
    // TODO Auto-generated method stub

  }

}
