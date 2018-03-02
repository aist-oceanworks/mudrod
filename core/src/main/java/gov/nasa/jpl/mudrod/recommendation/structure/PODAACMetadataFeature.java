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

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.regex.Pattern;

public class PODAACMetadataFeature extends MetadataFeature {

	public void normalizeSpatialVariables(Map<String, Object> metadata, Map<String, Object> updatedValues) {

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

	public void normalizeTemporalVariables(Map<String, Object> metadata, Map<String, Object> updatedValues) {
		String trStr = (String) metadata.get("Dataset-TemporalResolution");
		if ("".equals(trStr)) {
			trStr = (String) metadata.get("Dataset-TemporalRepeat");
		}

		updatedValues.put("Dataset-Derivative-TemporalResolution", covertTimeUnit(trStr));
	}

	public void normalizeOtherVariables(Map<String, Object> metadata, Map<String, Object> updatedValues) {
		String shortname = (String) metadata.get("Dataset-ShortName");
		double versionNUm = getVersionNum(shortname);
		updatedValues.put("Dataset-Derivative-VersionNum", versionNUm);
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

	@Override
	public void initFeatureType() {
		// TODO Auto-generated method stub
		//Map<String, Integer> featureTypes = new HashMap<>();
		featureTypes.put("DatasetParameter-Variable", VAR_CATEGORICAL);
		featureTypes.put("DatasetRegion-Region", VAR_CATEGORICAL);
		featureTypes.put("Dataset-ProjectionType", VAR_CATEGORICAL);
		featureTypes.put("Dataset-ProcessingLevel", VAR_CATEGORICAL);
		featureTypes.put("DatasetParameter-Topic", VAR_CATEGORICAL);
		featureTypes.put("DatasetParameter-Term", VAR_CATEGORICAL);
		featureTypes.put("DatasetParameter-Category", VAR_CATEGORICAL);
		featureTypes.put("DatasetPolicy-DataFormat", VAR_CATEGORICAL);
		featureTypes.put("Collection-ShortName", VAR_CATEGORICAL);
		featureTypes.put("DatasetSource-Source-Type", VAR_CATEGORICAL);
		featureTypes.put("DatasetSource-Source-ShortName", VAR_CATEGORICAL);
		featureTypes.put("DatasetSource-Sensor-ShortName", VAR_CATEGORICAL);
		featureTypes.put("DatasetPolicy-Availability", VAR_CATEGORICAL);
		featureTypes.put("Dataset-Provider-ShortName", VAR_CATEGORICAL);
		featureTypes.put("Dataset-Derivative-ProcessingLevel", VAR_ORDINAL);
		featureTypes.put("Dataset-Derivative-TemporalResolution", VAR_ORDINAL);
		featureTypes.put("Dataset-Derivative-SpatialResolution", VAR_ORDINAL);
	}

	@Override
	public void initFeatureWeight() {
		// TODO Auto-generated method stub
		//Map<String, Integer> featureWeights = new HashMap<>();
		featureWeights.put("Dataset-Derivative-ProcessingLevel", 5);
		featureWeights.put("DatasetParameter-Category", 5);
		featureWeights.put("DatasetParameter-Variable", 5);
		featureWeights.put("DatasetSource-Sensor-ShortName", 5);
		featureWeights.put("DatasetPolicy-Availability", 4);
		featureWeights.put("DatasetRegion-Region", 4);
		featureWeights.put("DatasetSource-Source-Type", 4);
		featureWeights.put("DatasetSource-Source-ShortName", 4);
		featureWeights.put("DatasetParameter-Term", 4);
		featureWeights.put("DatasetPolicy-DataFormat", 4);
		featureWeights.put("Dataset-Derivative-SpatialResolution", 4);
		featureWeights.put("Temporal_Covergae", 4);
		featureWeights.put("DatasetParameter-Topic", 3);
		featureWeights.put("Collection-ShortName", 3);
		featureWeights.put("Dataset-Derivative-TemporalResolution", 3);
		featureWeights.put("Spatial_Covergae", 3);
		featureWeights.put("Dataset-ProjectionType", 1);
		featureWeights.put("Dataset-Provider-ShortName", 1);
	}

	@Override
	public void spatialSimilarity(Map<String, Object> metadataA, Map<String, Object> metadataB,
			XContentBuilder contentBuilder) {
		// TODO Auto-generated method stub
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

		try {
			contentBuilder.field("Spatial_Covergae_Sim", similarity);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void temporalSimilarity(Map<String, Object> metadataA, Map<String, Object> metadataB,
			XContentBuilder contentBuilder) {
		// TODO Auto-generated method stub
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
		try {
			contentBuilder.field("Temporal_Covergae_Sim", similarity);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void categoricalVariablesSimilarity(Map<String, Object> metadataA, Map<String, Object> metadataB,
			XContentBuilder contentBuilder) {
		// TODO Auto-generated method stub
		for (String variable : featureTypes.keySet()) {
			Integer type = featureTypes.get(variable);
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

			try {
				contentBuilder.field(variable + "_Sim", similarity);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@Override
	public void ordinalVariablesSimilarity(Map<String, Object> metadataA, Map<String, Object> metadataB,
			XContentBuilder contentBuilder) {
		// TODO Auto-generated method stub
		for (String variable : featureTypes.keySet()) {
			Integer type = featureTypes.get(variable);
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

			try {
				contentBuilder.field(variable + "_Sim", similarity);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
