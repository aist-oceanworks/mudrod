package gov.nasa.jpl.mudrod.recommendation.process;

import gov.nasa.jpl.mudrod.discoveryengine.DiscoveryStepAbstract;
import gov.nasa.jpl.mudrod.driver.ESDriver;
import gov.nasa.jpl.mudrod.driver.SparkDriver;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class VariableBasedSimilarity extends DiscoveryStepAbstract implements Serializable {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(VariableBasedSimilarity.class);

  private DecimalFormat df = new DecimalFormat("#.000");
  // a map from variable to its type
  public Map<String, Integer> variableTypes;
  public Map<String, Integer> variableWeights;

  private static final Integer VAR_SPATIAL = 1;
  private static final Integer VAR_TEMPORAL = 2;
  private static final Integer VAR_CATEGORICAL = 3;
  private static final Integer VAR_ORDINAL = 4;

  // index name
  private String indexName;
  // type name of metadata in ES
  private String metadataType;
  private String variableSimType;

  /**
   * Creates a new instance of OHEncoder.
   *
   * @param props the Mudrod configuration
   * @param es    an instantiated {@link ESDriver}
   * @param spark an instantiated {@link SparkDriver}
   */
  public VariableBasedSimilarity(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);

    indexName = props.getProperty("indexName");
    metadataType = props.getProperty("recom_metadataType");
    variableSimType = props.getProperty("metadataCodeSimType");
    this.inital();
  }

  @Override
  public Object execute() {
    LOG.info("*****************calculating metadata variables based similarity starts******************");
    startTime = System.currentTimeMillis();
    es.deleteType(indexName, variableSimType);
    addMapping(es, indexName, variableSimType);

    VariableBasedSimilarity(es);
    es.refreshIndex();
    normalizeVariableWeight(es);
    es.refreshIndex();
    endTime = System.currentTimeMillis();
    LOG.info("*****************calculating metadata variables based similarity ends******************Took {}s", (endTime - startTime) / 1000);
    return null;
  }

  @Override
  public Object execute(Object o) {
    return null;
  }

  public void inital() {
    this.initVariableType();
    this.initVariableWeight();
  }

  private void initVariableType() {
    variableTypes = new HashMap<>();

    variableTypes.put("DatasetParameter-Variable", VAR_CATEGORICAL);
    variableTypes.put("DatasetRegion-Region", VAR_CATEGORICAL);
    variableTypes.put("Dataset-ProjectionType", VAR_CATEGORICAL);
    variableTypes.put("Dataset-ProcessingLevel", VAR_CATEGORICAL);
    variableTypes.put("DatasetParameter-Topic", VAR_CATEGORICAL);
    variableTypes.put("DatasetParameter-Term", VAR_CATEGORICAL);
    variableTypes.put("DatasetParameter-Category", VAR_CATEGORICAL);
    variableTypes.put("DatasetPolicy-DataFormat", VAR_CATEGORICAL);
    variableTypes.put("Collection-ShortName", VAR_CATEGORICAL);
    variableTypes.put("DatasetSource-Source-Type", VAR_CATEGORICAL);
    variableTypes.put("DatasetSource-Source-ShortName", VAR_CATEGORICAL);
    variableTypes.put("DatasetSource-Sensor-ShortName", VAR_CATEGORICAL);
    variableTypes.put("DatasetPolicy-Availability", VAR_CATEGORICAL);
    variableTypes.put("Dataset-Provider-ShortName", VAR_CATEGORICAL);

    variableTypes.put("Dataset-Derivative-ProcessingLevel", VAR_ORDINAL);
    variableTypes.put("Dataset-Derivative-TemporalResolution", VAR_ORDINAL);
    variableTypes.put("Dataset-Derivative-SpatialResolution", VAR_ORDINAL);
  }

  private void initVariableWeight() {
    variableWeights = new HashMap<>();

    variableWeights.put("Dataset-Derivative-ProcessingLevel", 5);
    variableWeights.put("DatasetParameter-Category", 5);
    variableWeights.put("DatasetParameter-Variable", 5);
    variableWeights.put("DatasetSource-Sensor-ShortName", 5);

    variableWeights.put("DatasetPolicy-Availability", 4);
    variableWeights.put("DatasetRegion-Region", 4);
    variableWeights.put("DatasetSource-Source-Type", 4);
    variableWeights.put("DatasetSource-Source-ShortName", 4);
    variableWeights.put("DatasetParameter-Term", 4);
    variableWeights.put("DatasetPolicy-DataFormat", 4);
    variableWeights.put("Dataset-Derivative-SpatialResolution", 4);
    variableWeights.put("Temporal_Covergae", 4);

    variableWeights.put("DatasetParameter-Topic", 3);
    variableWeights.put("Collection-ShortName", 3);
    variableWeights.put("Dataset-Derivative-TemporalResolution", 3);
    variableWeights.put("Spatial_Covergae", 3);

    variableWeights.put("Dataset-ProjectionType", 1);
    variableWeights.put("Dataset-Provider-ShortName", 1);
  }

  public void VariableBasedSimilarity(ESDriver es) {

    es.createBulkProcessor();

    List<Map<String, Object>> metadatas = new ArrayList<>();
    SearchResponse scrollResp = es.getClient().prepareSearch(indexName).setTypes(metadataType).setScroll(new TimeValue(60000)).setQuery(QueryBuilders.matchAllQuery()).setSize(100).execute()
        .actionGet();
    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> metadataA = hit.getSource();
        metadatas.add(metadataA);
      }

      scrollResp = es.getClient().prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }
    }

    int size = metadatas.size();
    for (int i = 0; i < size; i++) {
      Map<String, Object> metadataA = metadatas.get(i);
      String shortNameA = (String) metadataA.get("Dataset-ShortName");

      for (int j = 0; j < size; j++) {
        metadataA = metadatas.get(i);
        Map<String, Object> metadataB = metadatas.get(j);
        String shortNameB = (String) metadataB.get("Dataset-ShortName");

        try {
          XContentBuilder contentBuilder = jsonBuilder().startObject();
          contentBuilder.field("concept_A", shortNameA);
          contentBuilder.field("concept_B", shortNameB);

          // spatial similarity
          this.spatialSimilarity(metadataA, metadataB, contentBuilder);
          // temporal similarity
          this.temporalSimilarity(metadataA, metadataB, contentBuilder);
          // categorical variables similarity
          this.categoricalVariablesSimilarity(metadataA, metadataB, contentBuilder);
          // ordinal variables similarity
          this.ordinalVariablesSimilarity(metadataA, metadataB, contentBuilder);

          contentBuilder.endObject();

          IndexRequest ir = new IndexRequest(indexName, variableSimType).source(contentBuilder);
          es.getBulkProcessor().add(ir);

        } catch (IOException e1) {
          e1.printStackTrace();
        }

      }
    }

    es.destroyBulkProcessor();
  }

  /*
   * refer to P. Frontiera, R. Larson, and J. Radke (2008) A comparison of
     geometric approaches to assessing spatial similarity for GIR.
     International Journal of Geographical Information Science,
     22(3)
   */
  public void spatialSimilarity(Map<String, Object> metadataA, Map<String, Object> metadataB, XContentBuilder contentBuilder) throws IOException {

    double topA = (double) metadataA.get("DatasetCoverage-Derivative-NorthLat");
    double bottomA = (double) metadataA.get("DatasetCoverage-Derivative-SouthLat");
    double leftA = (double) metadataA.get("DatasetCoverage-Derivative-WestLon");
    double rightA = (double) metadataA.get("DatasetCoverage-Derivative-EastLon");
    double areaA = (double) metadataA.get("DatasetCoverage-Derivative-Area");

    double topB = (double) metadataB.get("DatasetCoverage-Derivative-NorthLat");
    double bottomB = (double) metadataB.get("DatasetCoverage-Derivative-SouthLat");
    double leftB = (double) metadataB.get("DatasetCoverage-Derivative-WestLon");
    double rightB = (double) metadataB.get("DatasetCoverage-Derivative-EastLon");
    double areaB = (double) metadataB.get("DatasetCoverage-Derivative-Area");

    // Intersect area
    double xOverlap = Math.max(0, Math.min(rightA, rightB) - Math.max(leftA, leftB));
    double yOverlap = Math.max(0, Math.min(topA, topB) - Math.max(bottomA, bottomB));
    double overlapArea = xOverlap * yOverlap;

    // Calculate coverage similarity
    double similarity = 0.0;
    if (areaA > 0 && areaB > 0) {
      similarity = (overlapArea / areaA + overlapArea / areaB) * 0.5;
    }

    contentBuilder.field("Spatial_Covergae_Sim", similarity);
  }

  public void temporalSimilarity(Map<String, Object> metadataA, Map<String, Object> metadataB, XContentBuilder contentBuilder) throws IOException {

    double similarity = 0.0;
    double startTimeA = Double.parseDouble((String) metadataA.get("Dataset-DatasetCoverage-StartTimeLong"));
    String endTimeAStr = (String) metadataA.get("Dataset-DatasetCoverage-StopTimeLong");
    double endTimeA = 0.0;
    if ("".equals(endTimeAStr)) {
      endTimeA = System.currentTimeMillis();
    } else {
      endTimeA = Double.parseDouble(endTimeAStr);
    }
    double timespanA = endTimeA - startTimeA;

    double startTimeB = Double.parseDouble((String) metadataB.get("Dataset-DatasetCoverage-StartTimeLong"));
    String endTimeBStr = (String) metadataB.get("Dataset-DatasetCoverage-StopTimeLong");
    double endTimeB = 0.0;
    if ("".equals(endTimeBStr)) {
      endTimeB = System.currentTimeMillis();
    } else {
      endTimeB = Double.parseDouble(endTimeBStr);
    }
    double timespanB = endTimeB - startTimeB;

    double intersect = 0.0;
    if (startTimeB >= endTimeA || endTimeB <= startTimeA) {
      intersect = 0.0;
    } else if (startTimeB >= startTimeA && endTimeB <= endTimeA) {
      intersect = timespanB;
    } else if (startTimeA >= startTimeB && endTimeA <= endTimeB) {
      intersect = timespanA;
    } else {
      intersect = (startTimeA > startTimeB) ? (endTimeB - startTimeA) : (endTimeA - startTimeB);
    }

    similarity = intersect / (Math.sqrt(timespanA) * Math.sqrt(timespanB));
    contentBuilder.field("Temporal_Covergae_Sim", similarity);
  }

  public void categoricalVariablesSimilarity(Map<String, Object> metadataA, Map<String, Object> metadataB, XContentBuilder contentBuilder) throws IOException {

    for (String variable : variableTypes.keySet()) {
      Integer type = variableTypes.get(variable);
      if (type != VAR_CATEGORICAL) {
        continue;
      }

      double similarity = 0.0;
      Object valueA = metadataA.get(variable);
      Object valueB = metadataB.get(variable);
      if (valueA instanceof ArrayList) {
        ArrayList<String> aList = (ArrayList<String>) valueA;
        ArrayList<String> bList = (ArrayList<String>) valueB;
        if (aList != null && bList != null) {

          int lengthA = aList.size();
          int lengthB = bList.size();
          List<String> newAList = new ArrayList<>(aList);
          List<String> newBList = new ArrayList<>(bList);
          newAList.retainAll(newBList);
          similarity = newAList.size() / lengthA;
        }

      } else if (valueA instanceof String) {
        if (valueA.equals(valueB)) {
          similarity = 1.0;
        }
      }

      contentBuilder.field(variable + "_Sim", similarity);
    }
  }

  public void ordinalVariablesSimilarity(Map<String, Object> metadataA, Map<String, Object> metadataB, XContentBuilder contentBuilder) throws IOException {
    for (String variable : variableTypes.keySet()) {
      Integer type = variableTypes.get(variable);
      if (type != VAR_ORDINAL) {
        continue;
      }

      double similarity = 0.0;
      Object valueA = metadataA.get(variable);
      Object valueB = metadataB.get(variable);
      if (valueA != null && valueB != null) {

        double a = (double) valueA;
        double b = (double) valueB;
        if (a != 0.0) {
          similarity = 1 - Math.abs(b - a) / a;
          if (similarity < 0) {
            similarity = 0.0;
          }
        }
      }

      contentBuilder.field(variable + "_Sim", similarity);
    }
  }

  public static void addMapping(ESDriver es, String index, String type) {
    XContentBuilder Mapping;
    try {
      Mapping = jsonBuilder().startObject().startObject(type).startObject("properties").startObject("concept_A").field("type", "string").field("index", "not_analyzed").endObject()
          .startObject("concept_B").field("type", "string").field("index", "not_analyzed").endObject()

          .endObject().endObject().endObject();

      es.getClient().admin().indices().preparePutMapping(index).setType(type).setSource(Mapping).execute().actionGet();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void normalizeVariableWeight(ESDriver es) {

    es.createBulkProcessor();

    double totalWeight = 0.0;
    for (String variable : variableWeights.keySet()) {
      totalWeight += variableWeights.get(variable);
    }

    SearchResponse scrollResp = es.getClient().prepareSearch(indexName).setTypes(variableSimType).setScroll(new TimeValue(60000)).setQuery(QueryBuilders.matchAllQuery()).setSize(100).execute()
        .actionGet();
    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> similarities = hit.getSource();

        double totalSim = 0.0;
        for (String variable : variableWeights.keySet()) {
          if (similarities.containsKey(variable + "_Sim")) {
            double value = (double) similarities.get(variable + "_Sim");
            double weight = variableWeights.get(variable);
            totalSim += weight * value;
          }
        }

        double weight = totalSim / totalWeight;
        UpdateRequest ur = es.generateUpdateRequest(indexName, variableSimType, hit.getId(), "weight", weight);
        es.getBulkProcessor().add(ur);
      }

      scrollResp = es.getClient().prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }
    }

    es.destroyBulkProcessor();
  }
}