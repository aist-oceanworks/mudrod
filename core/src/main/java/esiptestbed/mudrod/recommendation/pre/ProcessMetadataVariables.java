package esiptestbed.mudrod.recommendation.pre;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;

public class ProcessMetadataVariables extends DiscoveryStepAbstract {

  private static final Logger LOG = LoggerFactory
      .getLogger(ProcessMetadataVariables.class);
  // index name
  private String indexName;
  // type name of metadata in ES
  private String metadataType;

  /**
   * Creates a new instance of OHEncoder.
   *
   * @param props
   *          the Mudrod configuration
   */
  public ProcessMetadataVariables(Properties props, ESDriver es,
      SparkDriver spark) {
    super(props, es, spark);
    indexName = props.getProperty("indexName");
    metadataType = props.getProperty("recom_metadataType");
  }

  @Override
  public Object execute() {
    // TODO Auto-generated method stub
    LOG.info(
        "*****************processing metadata variables starts******************");
    startTime = System.currentTimeMillis();

    processVariables(es);

    endTime = System.currentTimeMillis();
    LOG.info(
        "*****************processing metadata variables ends******************Took {}s",
        (endTime - startTime) / 1000);

    return null;
  }

  @Override
  public Object execute(Object o) {
    // TODO Auto-generated method stub
    return null;
  }

  public void processVariables(ESDriver es) {

    es.createBulkProcessor();

    SearchResponse scrollResp = es.getClient().prepareSearch(indexName)
        .setTypes(metadataType).setScroll(new TimeValue(60000))
        .setQuery(QueryBuilders.matchAllQuery()).setSize(100).execute()
        .actionGet();
    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> metadata = hit.getSource();
        Map<String, Object> updatedValues = new HashMap<String, Object>();

        // Transform Longitude and calculate area
        double top = ParseDouble(
            (String) metadata.get("DatasetCoverage-NorthLat"));
        double bottom = ParseDouble(
            (String) metadata.get("DatasetCoverage-SouthLat"));
        double left = ParseDouble(
            (String) metadata.get("DatasetCoverage-WestLon"));
        double right = ParseDouble(
            (String) metadata.get("DatasetCoverage-EastLon"));

        if (left > 180) {
          left = left - 360;
        }

        if (right > 180) {
          right = right - 360;
        }

        updatedValues.put("DatasetCoverage-EastLon-Double", right);
        updatedValues.put("DatasetCoverage-WestLon-Double", left);
        updatedValues.put("DatasetCoverage-NorthLat-Double", top);
        updatedValues.put("DatasetCoverage-SouthLat-Double", bottom);

        double area = (top - bottom) * (right - left);
        updatedValues.put("DatasetCoverage-Area", area);

        UpdateRequest ur = es.generateUpdateRequest(indexName, metadataType,
            hit.getId(), updatedValues);
        es.getBulkProcessor().add(ur);
        // temporal similarity
      }

      scrollResp = es.getClient().prepareSearchScroll(scrollResp.getScrollId())
          .setScroll(new TimeValue(600000)).execute().actionGet();
      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }
    }

    es.destroyBulkProcessor();
  }

  double ParseDouble(String strNumber) {
    if (strNumber != null && strNumber.length() > 0) {
      try {
        return Double.parseDouble(strNumber);
      } catch (Exception e) {
        return -1; // or some value to mark this field is wrong. or make a
        // function validates field first ...
      }
    } else
      return 0;
  }

}
