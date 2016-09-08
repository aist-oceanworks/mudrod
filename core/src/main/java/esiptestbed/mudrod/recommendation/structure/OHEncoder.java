package esiptestbed.mudrod.recommendation.structure;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import esiptestbed.mudrod.driver.ESDriver;

public class OHEncoder {

  private String indexName;
  public List<String> CategoricalVars;
  public Map<String, Double> CategoricalVarWeights;
  private Map<String, Map<String, Vector>> CategoricalVarValueVecs;
  private String metadataType;

  private String VAR_NOT_EXIST = "varNotExist";

  public OHEncoder(Properties props) {
    indexName = props.getProperty("indexName");
    metadataType = props.getProperty("recom_metadataType");

    this.inital();
  }

  public OHEncoder() {

    this.inital();
  }

  public void inital() {

    CategoricalVarValueVecs = new HashMap<String, Map<String, Vector>>();
    CategoricalVarWeights = new HashMap<String, Double>();   

    CategoricalVarWeights.put("DatasetParameter-Topic", 3.0);
    CategoricalVarWeights.put("DatasetParameter-Term", 4.0);
    CategoricalVarWeights.put("DatasetParameter-Category", 5.0);
    CategoricalVarWeights.put("DatasetParameter-Variable", 5.0);
    CategoricalVarWeights.put("Dataset-ProcessingLevel_facet", 5.0);
    CategoricalVarWeights.put("DatasetSource-Source-Type", 4.0);
    CategoricalVarWeights.put("DatasetSource-Source-ShortName", 4.0);
    CategoricalVarWeights.put("DatasetSource-Sensor-ShortName", 5.0);
    CategoricalVarWeights.put("DatasetSource-Sensor-SwathWidth", 3.0);

    CategoricalVarWeights.put("DatasetRegion-Region", 4.0);
    CategoricalVarWeights.put("Dataset-ProjectionType", 1.0);
    CategoricalVarWeights.put("DatasetCoverage-NorthLat_facet", 3.0);
    CategoricalVarWeights.put("DatasetCoverage-SouthLat_facet", 3.0);
    CategoricalVarWeights.put("DatasetCoverage-WestLon_facet", 3.0);
    CategoricalVarWeights.put("DatasetCoverage-EastLon_facet", 3.0);
    CategoricalVarWeights.put("Dataset-ProjectionType", 1.0);
    CategoricalVarWeights.put("Dataset-HorizontalResolutionRange", 1.0);
    CategoricalVarWeights.put("Dataset-LatitudeResolution", 4.0);
    CategoricalVarWeights.put("Dataset-LongitudeResolution", 4.0);
    CategoricalVarWeights.put("Dataset-SwathWidth", 4.0);

    CategoricalVarWeights.put("Dataset-SatelliteSpatialResolution", 3.0);
    CategoricalVarWeights.put("Dataset-AcrossTrackResolution", 4.0);
    CategoricalVarWeights.put("Dataset-AlongTrackResolution", 4.0);
    CategoricalVarWeights.put("Dataset-TemporalRepeat", 3.0);
    CategoricalVarWeights.put("Dataset-TemporalResolution-Group", 2.0);
    CategoricalVarWeights.put("Dataset-TemporalRepeatMin", 2.0);
    CategoricalVarWeights.put("Dataset-TemporalResolutionRange", 2.0);
    CategoricalVarWeights.put("Dataset-TemporalResolution", 3.0);
    CategoricalVarWeights.put("Dataset-TemporalRepeatMax", 2.0);
    CategoricalVarWeights.put("Dataset-DatasetCoverage-TimeSpan", 4.0);
    CategoricalVarWeights.put("DatasetPolicy-DataLatency", 3.0);
    CategoricalVarWeights.put("DatasetPolicy-DataFrequency", 3.0);
    CategoricalVarWeights.put("DatasetPolicy-DataDuration", 1.0);
    CategoricalVarWeights.put("DatasetPolicy-DataFormat", 4.0);
    CategoricalVarWeights.put("DatasetPolicy-Availability", 4.0);
    CategoricalVarWeights.put("Collection-ShortName", 3.0);
    CategoricalVarWeights.put("Dataset-Provider-ShortName", 1.0);
    CategoricalVars = new ArrayList(CategoricalVarWeights.keySet());
  }

  public void OHEncodeaAllMetadata(ESDriver es) {

    es.createBulkProcesser();

    SearchResponse scrollResp = es.getClient().prepareSearch(indexName)
        .setTypes(metadataType).setScroll(new TimeValue(60000))
        .setQuery(QueryBuilders.matchAllQuery()).setSize(100).execute()
        .actionGet();
    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> metadata = hit.getSource();
        Map<String, Object> metadatacode;
        try {
          metadatacode = OHEncodeMetadata(es, metadata);
          UpdateRequest ur = es.genUpdateRequest(indexName, metadataType,
              hit.getId(), metadatacode);
          es.getBulkProcessor().add(ur);
        } catch (InterruptedException | ExecutionException e1) {
          e1.printStackTrace();
        }
      }

      scrollResp = es.getClient().prepareSearchScroll(scrollResp.getScrollId())
          .setScroll(new TimeValue(600000)).execute().actionGet();
      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }
    }

    es.destroyBulkProcessor();
  }

  private Map<String, Object> OHEncodeMetadata(ESDriver es,
      Map<String, Object> metadata)
      throws InterruptedException, ExecutionException {
    Map<String, Object> metadataCodes = new HashMap<String, Object>();
    int CategoryNum = CategoricalVars.size();
    for (int i = 0; i < CategoryNum; i++) {
      String var = CategoricalVars.get(i);
      Vector vec = null;
      if (metadata.get(var) != null && metadata.get(var) != "") {
        String value = es.customAnalyzing(indexName, "csv",
            metadata.get(var).toString());
        if (value.contains(",")) {

          String[] values = value.split(",");
          int valuenum = values.length;
          Vector tmpvec = null;
          for (int j = 0; j < valuenum; j++) {
            tmpvec = getValueVec(var, values[j]);
            if (vec == null) {
              vec = tmpvec;
            } else {
              vec = this.VectorSum(vec, tmpvec);
            }
          }
        } else {
          vec = getValueVec(var, value);
          if (vec == null) {
            vec = getValueVec(var, VAR_NOT_EXIST);
          }
        }
      } else {
        vec = getValueVec(var, VAR_NOT_EXIST);
      }

      double[] codeArr = vec.toArray();

      String codeStr = Arrays.toString(codeArr);
      codeStr = codeStr.substring(1, codeStr.length() - 1);
      metadataCodes.put(var + "_code", codeStr);
    }

    return metadataCodes;
  }

  private Vector getValueVec(String var, String value) {

    if (value.startsWith("[")) {
      value = value.substring(1, value.length());
    }

    if (value.endsWith("]")) {
      value = value.substring(0, value.length() - 1);
    }

    Vector tmpvec = CategoricalVarValueVecs.get(var).get(value);
    if (tmpvec == null) {
      tmpvec = CategoricalVarValueVecs.get(var).get(VAR_NOT_EXIST);
    }
    return tmpvec;
  }

  public void OHEncodeVars(ESDriver es) {
    int CategoryNum = CategoricalVars.size();
    for (int i = 0; i < CategoryNum; i++) {
      String var = CategoricalVars.get(i);
      Map<String, Vector> valueVecs = this.OHEncodeVar(es, var);
      CategoricalVarValueVecs.put(var, valueVecs);
    }
  }

  private Map<String, Vector> OHEncodeVar(ESDriver es, String varName) {

    SearchResponse sr = es.getClient().prepareSearch(indexName)
        .setTypes(metadataType).setQuery(QueryBuilders.matchAllQuery())
        .setSize(0)
        .addAggregation(
            AggregationBuilders.terms("Values").field(varName).size(0))
        .execute().actionGet();
    Terms VarValues = sr.getAggregations().get("Values");

    Map<String, Vector> valueVec = new HashMap<String, Vector>();
    int valueNum = VarValues.getBuckets().size();
    int pos = 0;
    for (Terms.Bucket entry : VarValues.getBuckets()) {
      Object obj = entry.getKey();
      String value = obj.toString();
      Vector sv = Vectors.sparse(valueNum, new int[] { pos },
          new double[] { 1 });
      pos += 1;
      valueVec.put(value, sv);
    }

    Vector sv = Vectors.sparse(valueNum, new int[] { 0 }, new double[] { 0 });
    valueVec.put(VAR_NOT_EXIST, sv);

    return valueVec;
  }

  private static Vector VectorSum(Vector v1, Vector v2) {
    double[] arr1 = v1.toArray();
    double[] arr2 = v2.toArray();

    int length = arr1.length;
    double[] arr3 = new double[length];
    for (int i = 0; i < length; i++) {
      double value = arr1[i] + arr2[i];
      if (value > 1.0) {
        value = 1.0;
      }
      arr3[i] = value;
    }

    Vector v3 = Vectors.dense(arr3);
    return v3;
  }
}
