package esiptestbed.mudrod.recommendation.process;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;

public class VariableBasedSimilarity extends DiscoveryStepAbstract
    implements Serializable {

  private static final Logger LOG = LoggerFactory
      .getLogger(VariableBasedSimilarity.class);

  private DecimalFormat df = new DecimalFormat("#.000");
  // a map from variable to its weight
  public Map<String, Integer> variableTypes;

  private static final Integer VAR_SPATIAL = 1;
  private static final Integer VAR_TEMPORAL = 2;
  private static final Integer VAR_CATEGORICAL = 3;
  private static final Integer VAR_ORDINAL = 4;

  // index name
  private String indexName;
  // type name of metadata in ES
  private String metadataType;
  private String variableSimType;
  // default value of variable if variable is null:
  private String VAR_NOT_EXIST = "varNotExist";

  /**
   * Creates a new instance of OHEncoder.
   *
   * @param props
   *          the Mudrod configuration
   */
  public VariableBasedSimilarity(Properties props, ESDriver es,
      SparkDriver spark) {
    super(props, es, spark);

    indexName = props.getProperty("indexName");
    metadataType = props.getProperty("recom_metadataType");
    variableSimType = props.getProperty("metadataCodeSimType");
    this.inital();
  }

  @Override
  public Object execute() {
    LOG.info(
        "*****************calculating metadata variables based similarity starts******************");
    startTime = System.currentTimeMillis();
    addMapping(es, indexName, variableSimType);
    VariableBasedSimilarity(es);

    endTime = System.currentTimeMillis();
    LOG.info(
        "*****************calculating metadata variables based similarity ends******************Took {}s",
        (endTime - startTime) / 1000);
    return null;
  }

  @Override
  public Object execute(Object o) {
    // TODO Auto-generated method stub
    return null;
  }

  public void inital() {

    variableTypes = new HashMap<String, Integer>();

    variableTypes.put("Dataset-DatasetCoverage-StopTimeLong", VAR_TEMPORAL);
    variableTypes.put("Dataset-DatasetCoverage-StartTimeLong", VAR_TEMPORAL);

    variableTypes.put("DatasetCoverage-NorthLat", VAR_SPATIAL);
    variableTypes.put("DatasetCoverage-SouthLat", VAR_SPATIAL);
    variableTypes.put("DatasetCoverage-WestLon", VAR_SPATIAL);
    variableTypes.put("DatasetCoverage-EastLon", VAR_SPATIAL);

    variableTypes.put("DatasetRegion-Region", VAR_CATEGORICAL);
    variableTypes.put("Dataset-ProjectionType", VAR_CATEGORICAL);
    variableTypes.put("Dataset-ProcessingLevel", VAR_CATEGORICAL);
    variableTypes.put("DatasetParameter-Variable", VAR_CATEGORICAL);
    variableTypes.put("DatasetParameter-Topic", VAR_CATEGORICAL);
    variableTypes.put("DatasetParameter-Term", VAR_CATEGORICAL);
    variableTypes.put("DatasetParameter-Category", VAR_CATEGORICAL);
    variableTypes.put("DatasetPolicy-DataFormat", VAR_CATEGORICAL);
    variableTypes.put("Collection-ShortName", VAR_CATEGORICAL);

    variableTypes.put("Dataset-LatitudeResolution", VAR_ORDINAL);
    variableTypes.put("Dataset-LongitudeResolution", VAR_ORDINAL);
    variableTypes.put("Dataset-TemporalResolution", VAR_ORDINAL);
    variableTypes.put("Dataset-TemporalResolution-Group", VAR_ORDINAL);
    variableTypes.put("Dataset-DatasetCoverage-TimeSpan", VAR_ORDINAL);

  }

  public void VariableBasedSimilarity(ESDriver es) {

    es.createBulkProcessor();

    SearchResponse scrollResp = es.getClient().prepareSearch(indexName)
        .setTypes(metadataType).setScroll(new TimeValue(60000))
        .setQuery(QueryBuilders.matchAllQuery()).setSize(100).execute()
        .actionGet();
    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> metadataA = hit.getSource();
        String shortNameA = (String) metadataA.get("Dataset-ShortName");
        // inner search
        SearchResponse inerscrollResp = es.getClient().prepareSearch(indexName)
            .setTypes(metadataType).setScroll(new TimeValue(60000))
            .setQuery(QueryBuilders.matchAllQuery()).setSize(100).execute()
            .actionGet();
        while (true) {
          for (SearchHit innerhit : inerscrollResp.getHits().getHits()) {
            Map<String, Object> metadataB = innerhit.getSource();
            String shortNameB = (String) metadataB.get("Dataset-ShortName");

            // spatial similarity
            double spatialSim = spatialSimilarity(metadataA, metadataB);
            // System.out.print(spatialSim + ", ");
            // temporal similarity

            // insert similarity based on variables
            IndexRequest ir = null;
            try {
              ir = new IndexRequest(indexName, variableSimType).source(
                  jsonBuilder().startObject().field("concept_A", shortNameA)
                      .field("concept_B", shortNameB)
                      .field("Weight", spatialSim).endObject());
            } catch (IOException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }
            es.getBulkProcessor().add(ir);

            // break;
          }

          inerscrollResp = es.getClient()
              .prepareSearchScroll(inerscrollResp.getScrollId())
              .setScroll(new TimeValue(600000)).execute().actionGet();
          if (inerscrollResp.getHits().getHits().length == 0) {
            break;
          }
        }

        // break;
      }

      scrollResp = es.getClient().prepareSearchScroll(scrollResp.getScrollId())
          .setScroll(new TimeValue(600000)).execute().actionGet();
      if (scrollResp.getHits().getHits().length == 0) {
        break;
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
  public double spatialSimilarity(Map<String, Object> metadataA,
      Map<String, Object> metadataB) {

    double topA = (double) metadataA.get("DatasetCoverage-NorthLat-Double");
    double bottomA = (double) metadataA.get("DatasetCoverage-SouthLat-Double");
    double leftA = (double) metadataA.get("DatasetCoverage-WestLon-Double");
    double rightA = (double) metadataA.get("DatasetCoverage-EastLon-Double");
    double areaA = (double) metadataA.get("DatasetCoverage-Area");

    double topB = (double) metadataB.get("DatasetCoverage-NorthLat-Double");
    double bottomB = (double) metadataB.get("DatasetCoverage-SouthLat-Double");
    double leftB = (double) metadataB.get("DatasetCoverage-WestLon-Double");
    double rightB = (double) metadataB.get("DatasetCoverage-EastLon-Double");
    double areaB = (double) metadataB.get("DatasetCoverage-Area");

    // Intersect area
    double x_overlap = Math.max(0,
        Math.min(rightA, rightB) - Math.max(leftA, leftB));
    double y_overlap = Math.max(0,
        Math.min(topA, topB) - Math.max(bottomA, bottomB));
    double overlapArea = x_overlap * y_overlap;

    // Calculate similarity
    double similarity = 0.0;
    if (areaA > 0 && areaB > 0) {
      similarity = (overlapArea / areaA + overlapArea / areaB) * 0.5;
    }

    return similarity;
  }

  public double temporalSimilarity() {

    return 0;
  }

  public static void addMapping(ESDriver es, String index, String type) {
    XContentBuilder Mapping;
    try {
      Mapping = jsonBuilder().startObject().startObject(type)
          .startObject("properties").startObject("concept_A")
          .field("type", "string").field("index", "not_analyzed").endObject()
          .startObject("concept_B").field("type", "string")
          .field("index", "not_analyzed").endObject()

          .endObject().endObject().endObject();

      es.getClient().admin().indices().preparePutMapping(index).setType(type)
          .setSource(Mapping).execute().actionGet();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
