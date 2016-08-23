package esiptestbed.mudrod.recommendation.process;

import java.util.List;
import java.util.Map;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.recommendation.structure.ItemSimCalculator;
import esiptestbed.mudrod.utils.LinkageTriple;

public class ItemBasedCF extends DiscoveryStepAbstract {

  public ItemBasedCF(Map<String, String> config, ESDriver es,
      SparkDriver spark) {
    super(config, es, spark);
    // TODO Auto-generated constructor stub
  }

  @Override
  public Object execute() {
    // TODO Auto-generated method stub
    try {
      String user_metadat_optFile = config.get("user_based_item_optMatrix");
      ItemSimCalculator simcal = new ItemSimCalculator(config);
      List<LinkageTriple> triples = simcal.CalItemSimfromMatrix(spark,
          user_metadat_optFile, 1);
      LinkageTriple.insertTriples(es, triples, config.get("indexName"),
          config.get("metadataItemBasedSimType"), true, false);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public Object execute(Object o) {
    // TODO Auto-generated method stub
    return null;
  }
}
