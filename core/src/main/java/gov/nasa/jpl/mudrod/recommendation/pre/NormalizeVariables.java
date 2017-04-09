package gov.nasa.jpl.mudrod.recommendation.pre;

import gov.nasa.jpl.mudrod.discoveryengine.DiscoveryStepAbstract;
import gov.nasa.jpl.mudrod.driver.ESDriver;
import gov.nasa.jpl.mudrod.driver.SparkDriver;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public class NormalizeVariables extends DiscoveryStepAbstract {

  /**
   *
   */
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(NormalizeVariables.class);
  // index name
  private String indexName;
  // type name of metadata in ES
  private String metadataType;

  /**
   * Creates a new instance of OHEncoder.
   *
   * @param props the Mudrod configuration
   * @param es    an instantiated {@link ESDriver}
   * @param spark an instantiated {@link SparkDriver}
   */
  public NormalizeVariables(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
    indexName = props.getProperty("indexName");
    metadataType = props.getProperty("recom_metadataType");
  }

  @Override
  public Object execute() {
    LOG.info("*****************processing metadata variables starts******************");
    startTime = System.currentTimeMillis();

    normalizeMetadataVariables(es);

    endTime = System.currentTimeMillis();
    LOG.info("*****************processing metadata variables ends******************Took {}s", (endTime - startTime) / 1000);

    return null;
  }

  @Override
  public Object execute(Object o) {
    return null;
  }

  public void normalizeMetadataVariables(ESDriver es) {

    es.createBulkProcessor();

    SearchResponse scrollResp = es.getClient().prepareSearch(indexName).setTypes(metadataType).setScroll(new TimeValue(60000)).setQuery(QueryBuilders.matchAllQuery()).setSize(100).execute()
        .actionGet();
    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> metadata = hit.getSource();
        Map<String, Object> updatedValues = new HashMap<>();

        this.normalizeSpatialVariables(metadata, updatedValues);
        this.normalizeTemporalVariables(metadata, updatedValues);
        this.normalizeOtherVariables(metadata, updatedValues);

        UpdateRequest ur = es.generateUpdateRequest(indexName, metadataType, hit.getId(), updatedValues);
        es.getBulkProcessor().add(ur);
      }

      scrollResp = es.getClient().prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }
    }

    es.destroyBulkProcessor();
  }

  private void normalizeOtherVariables(Map<String, Object> metadata, Map<String, Object> updatedValues) {
    String shortname = (String) metadata.get("Dataset-ShortName");
    double versionNUm = getVersionNum(shortname);
    updatedValues.put("Dataset-Derivative-VersionNum", versionNUm);

  }

  private Double getVersionNum(String version) {
    if (version == null) {
      return 0.0;
    }
    Double versionNum = 0.0;
    Pattern p = Pattern.compile(".*[a-zA-Z].*");
    if ("Operational/Near-Real-Time".equals(version)) {
      versionNum = 2.0;
    } else if (version.matches("[0-9]{1}[a-zA-Z]{1}")) {
      versionNum = Double.parseDouble(version.substring(0, 1));
    } else if (p.matcher(version).find()) {
      versionNum = 0.0;
    } else {
      versionNum = Double.parseDouble(version);
      if (versionNum >= 5) {
        versionNum = 20.0;
      }
    }
    return versionNum;
  }

  private void normalizeSpatialVariables(Map<String, Object> metadata, Map<String, Object> updatedValues) {

    // get spatial resolution
    Double spatialR;
    if (metadata.get("Dataset-SatelliteSpatialResolution") != null) {
      spatialR = (Double) metadata.get("Dataset-SatelliteSpatialResolution");
    } else {
      Double gridR = (Double) metadata.get("Dataset-GridSpatialResolution");
      if (gridR != null) {
        spatialR = 111 * gridR;
      } else {
        spatialR = 25.0;
      }
    }
    updatedValues.put("Dataset-Derivative-SpatialResolution", spatialR);

    // Transform Longitude and calculate coverage area
    double top = parseDouble((String) metadata.get("DatasetCoverage-NorthLat"));
    double bottom = parseDouble((String) metadata.get("DatasetCoverage-SouthLat"));
    double left = parseDouble((String) metadata.get("DatasetCoverage-WestLon"));
    double right = parseDouble((String) metadata.get("DatasetCoverage-EastLon"));

    if (left > 180) {
      left = left - 360;
    }

    if (right > 180) {
      right = right - 360;
    }

    if (left == right) {
      left = -180;
      right = 180;
    }

    double area = (top - bottom) * (right - left);

    updatedValues.put("DatasetCoverage-Derivative-EastLon", right);
    updatedValues.put("DatasetCoverage-Derivative-WestLon", left);
    updatedValues.put("DatasetCoverage-Derivative-NorthLat", top);
    updatedValues.put("DatasetCoverage-Derivative-SouthLat", bottom);
    updatedValues.put("DatasetCoverage-Derivative-Area", area);

    // get processing level
    String processingLevel = (String) metadata.get("Dataset-ProcessingLevel");
    double dProLevel = this.getProLevelNum(processingLevel);
    updatedValues.put("Dataset-Derivative-ProcessingLevel", dProLevel);
  }

  private void normalizeTemporalVariables(Map<String, Object> metadata, Map<String, Object> updatedValues) {

    String trStr = (String) metadata.get("Dataset-TemporalResolution");
    if ("".equals(trStr)) {
      trStr = (String) metadata.get("Dataset-TemporalRepeat");
    }

    updatedValues.put("Dataset-Derivative-TemporalResolution", covertTimeUnit(trStr));
  }

  private Double covertTimeUnit(String str) {
    Double timeInHour;
    if (str.contains("Hour")) {
      timeInHour = Double.parseDouble(str.split(" ")[0]);
    } else if (str.contains("Day")) {
      timeInHour = Double.parseDouble(str.split(" ")[0]) * 24;
    } else if (str.contains("Week")) {
      timeInHour = Double.parseDouble(str.split(" ")[0]) * 24 * 7;
    } else if (str.contains("Month")) {
      timeInHour = Double.parseDouble(str.split(" ")[0]) * 24 * 7 * 30;
    } else if (str.contains("Year")) {
      timeInHour = Double.parseDouble(str.split(" ")[0]) * 24 * 7 * 30 * 365;
    } else {
      timeInHour = 0.0;
    }

    return timeInHour;
  }

  public Double getProLevelNum(String pro) {
    if (pro == null) {
      return 1.0;
    }
    Double proNum = 0.0;
    Pattern p = Pattern.compile(".*[a-zA-Z].*");
    if (pro.matches("[0-9]{1}[a-zA-Z]{1}")) {
      proNum = Double.parseDouble(pro.substring(0, 1));
    } else if (p.matcher(pro).find()) {
      proNum = 1.0;
    } else {
      proNum = Double.parseDouble(pro);
    }

    return proNum;
  }

  private double parseDouble(String strNumber) {
    if (strNumber != null && strNumber.length() > 0) {
      try {
        return Double.parseDouble(strNumber);
      } catch (Exception e) {
        return -1;
      }
    } else
      return 0;
  }
}
