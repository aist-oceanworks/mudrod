package gov.nasa.jpl.mudrod.recommendation.structure;

import gov.nasa.jpl.mudrod.driver.ESDriver;
import gov.nasa.jpl.mudrod.driver.SparkDriver;
import gov.nasa.jpl.mudrod.main.MudrodConstants;
import gov.nasa.jpl.mudrod.utils.LabeledRowMatrix;
import gov.nasa.jpl.mudrod.utils.MatrixUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import scala.Tuple2;
import scala.tools.nsc.transform.SpecializeTypes.Abstract;

import java.io.Serializable;
import java.util.*;

public abstract class MetadataFeature implements Serializable {
	
	protected static final Integer VAR_SPATIAL = 1;
	protected static final Integer VAR_TEMPORAL = 2;
	protected static final Integer VAR_CATEGORICAL = 3;
	protected static final Integer VAR_ORDINAL = 4;
	
	public Map<String, Integer> featureTypes = new HashMap<>();
	public Map<String, Integer> featureWeights = new HashMap<>();

	public void normalizeMetadataVariables(Map<String, Object> metadata, Map<String, Object> updatedValues) {

		this.normalizeSpatialVariables(metadata, updatedValues);
		this.normalizeTemporalVariables(metadata, updatedValues);
		this.normalizeOtherVariables(metadata, updatedValues);
	}
	
	public void inital() {
	    this.initFeatureType();
	    this.initFeatureWeight();
	}
	
	public void featureSimilarity(Map<String, Object> metadataA, Map<String, Object> metadataB, XContentBuilder contentBuilder) {
		 this.spatialSimilarity(metadataA, metadataB, contentBuilder);
		 this.temporalSimilarity(metadataA, metadataB, contentBuilder);
		 this.categoricalVariablesSimilarity(metadataA, metadataB, contentBuilder);
		 this.ordinalVariablesSimilarity(metadataA, metadataB, contentBuilder);
	}

	/* for normalization */
	public abstract void normalizeSpatialVariables(Map<String, Object> metadata, Map<String, Object> updatedValues);

	public abstract void normalizeTemporalVariables(Map<String, Object> metadata, Map<String, Object> updatedValues);

	public abstract void normalizeOtherVariables(Map<String, Object> metadata, Map<String, Object> updatedValues);

	/* for similarity */
	public abstract void initFeatureType();

	public abstract void initFeatureWeight();
	
	public abstract void spatialSimilarity(Map<String, Object> metadataA, Map<String, Object> metadataB, XContentBuilder contentBuilder);
  
	public abstract void temporalSimilarity(Map<String, Object> metadataA, Map<String, Object> metadataB, XContentBuilder contentBuilder);

	public abstract void categoricalVariablesSimilarity(Map<String, Object> metadataA, Map<String, Object> metadataB, XContentBuilder contentBuilder);
  
	public abstract void ordinalVariablesSimilarity(Map<String, Object> metadataA, Map<String, Object> metadataB, XContentBuilder contentBuilder);
}
