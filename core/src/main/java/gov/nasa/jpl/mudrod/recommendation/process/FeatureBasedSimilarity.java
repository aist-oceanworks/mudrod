package gov.nasa.jpl.mudrod.recommendation.process;

import gov.nasa.jpl.mudrod.discoveryengine.DiscoveryStepAbstract;
import gov.nasa.jpl.mudrod.driver.ESDriver;
import gov.nasa.jpl.mudrod.driver.SparkDriver;
import gov.nasa.jpl.mudrod.main.MudrodConstants;
import gov.nasa.jpl.mudrod.recommendation.structure.MetadataFeature;
import gov.nasa.jpl.mudrod.recommendation.structure.PODAACMetadataFeature;

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

public class FeatureBasedSimilarity extends DiscoveryStepAbstract implements Serializable {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(FeatureBasedSimilarity.class);

  private DecimalFormat df = new DecimalFormat("#.000");
  // a map from variable to its type
  MetadataFeature metadata = null;
  public Map<String, Integer> variableTypes;
  public Map<String, Integer> variableWeights;


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
  public FeatureBasedSimilarity(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);

    indexName = props.getProperty(MudrodConstants.ES_INDEX_NAME);
    metadataType = MudrodConstants.RECOM_METADATA_TYPE;
    variableSimType = MudrodConstants.METADATA_FEATURE_SIM_TYPE;
 
    // !!! important, please change to other class when using other metadata
    metadata = new PODAACMetadataFeature();
    metadata.inital();
    variableTypes = metadata.featureTypes;
    variableWeights = metadata.featureWeights;
  }

  @Override
  public Object execute() {
    LOG.info("*****************calculating metadata feature based similarity starts******************");
    startTime = System.currentTimeMillis();
    es.deleteType(indexName, variableSimType);
    addMapping(es, indexName, variableSimType);

    featureSimilarity(es);
    es.refreshIndex();
    normalizeVariableWeight(es);
    es.refreshIndex();
    endTime = System.currentTimeMillis();
    LOG.info("*****************calculating metadata feature based similarity ends******************Took {}s", (endTime - startTime) / 1000);
    return null;
  }

  @Override
  public Object execute(Object o) {
    return null;
  }

  public void featureSimilarity(ESDriver es) {

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
      String shortNameA = (String) metadataA.get(props.getProperty(MudrodConstants.METADATA_ID));
      for (int j = 0; j < size; j++) {
    	metadataA = metadatas.get(i);
        Map<String, Object> metadataB = metadatas.get(j);
        String shortNameB = (String) metadataB.get(props.getProperty(MudrodConstants.METADATA_ID));

        try {
          XContentBuilder contentBuilder = jsonBuilder().startObject();
          contentBuilder.field("concept_A", shortNameA);
          contentBuilder.field("concept_B", shortNameB);

          // feature similarity
          metadata.featureSimilarity(metadataA, metadataB, contentBuilder);

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