package esiptestbed.mudrod.recommendation.pre;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.recommendation.structure.OHEncoder;

// one binary encoding of metadata parameters
public class OHEncodeMetadata extends DiscoveryStepAbstract {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory
      .getLogger(OHEncodeMetadata.class);

  public OHEncodeMetadata(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  @Override
  public Object execute() {
    LOG.info("*****************Metadata OHEncode starts******************");
    startTime = System.currentTimeMillis();

    OHEncoder encoder = new OHEncoder(props);
    encoder.OHEncodeVars(es);
    encoder.OHEncodeaAllMetadata(es);

    endTime = System.currentTimeMillis();
    es.refreshIndex();
    LOG.info(
        "*****************Metadata OHEncode ends******************Took {}s",
        (endTime - startTime) / 1000);
    return null;
  }

  @Override
  public Object execute(Object o) {
    // TODO Auto-generated method stub
    return null;
  }
}
