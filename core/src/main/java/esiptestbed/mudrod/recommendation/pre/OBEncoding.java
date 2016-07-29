package esiptestbed.mudrod.recommendation.pre;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.SparseVector;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.codehaus.jettison.json.JSONObject;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.Seconds;
import org.elasticsearch.common.joda.time.format.DateTimeFormatter;
import org.elasticsearch.common.joda.time.format.ISODateTimeFormat;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;

// one binary encoding of metadata parameters
public class OBEncoding extends DiscoveryStepAbstract{

	private List<String> CategoricalVars;
	private Map<String, Map<String, Vector>> CategoricalVarValueVecs;
	private String metadataType;
	
	private String VAR_NOT_EXIST = "varNotExist";
	
	public OBEncoding(Map<String, String> config, ESDriver es, SparkDriver spark) {
		super(config, es, spark);
		
		metadataType = config.get("recom_metadataType");
		CategoricalVarValueVecs = new HashMap<String, Map<String, Vector>>();
		CategoricalVars = new ArrayList<String>();

		CategoricalVars.add("Dataset-TemporalRepeat");
		CategoricalVars.add("Dataset-AcrossTrackResolution");
		CategoricalVars.add("Dataset-SatelliteSpatialResolution");
		CategoricalVars.add("Dataset-ProjectionType");
		CategoricalVars.add("Dataset-LongitudeResolution");
		CategoricalVars.add("Dataset-SwathWidth");
	
		CategoricalVars.add("Dataset-TemporalResolution-Group");
		CategoricalVars.add("Dataset-TemporalRepeatMin");
		CategoricalVars.add("Dataset-TemporalResolutionRange");
		CategoricalVars.add("Dataset-TemporalResolution");
		CategoricalVars.add("Dataset-TemporalRepeatMax");
		
		CategoricalVars.add("Dataset-ProcessingLevel");
		CategoricalVars.add("Dataset-HorizontalResolutionRange");
		CategoricalVars.add("Dataset-AlongTrackResolution");
		CategoricalVars.add("Dataset-LatitudeResolution");
		CategoricalVars.add("Dataset-Provider-ShortName");
		CategoricalVars.add("Dataset-DatasetCoverage-TimeSpan");
		CategoricalVars.add("DatasetRegion-Region");
		
		CategoricalVars.add("DatasetCoverage-NorthLat");
		CategoricalVars.add("DatasetCoverage-SouthLat");
		CategoricalVars.add("DatasetCoverage-WestLon");
		CategoricalVars.add("DatasetCoverage-EastLon");
		CategoricalVars.add("DatasetParameter-Category");
		CategoricalVars.add("DatasetParameter-Variable");
		CategoricalVars.add("DatasetParameter-Topic");
		CategoricalVars.add("DatasetParameter-Term");
		
		CategoricalVars.add("DatasetPolicy-DataDuration");
		CategoricalVars.add("DatasetPolicy-DataFormat");
		CategoricalVars.add("DatasetPolicy-DataLatency");
		CategoricalVars.add("DatasetPolicy-DataFrequency");
		CategoricalVars.add("DatasetPolicy-Availability");
		
		CategoricalVars.add("DatasetSource-Source-Type");
		CategoricalVars.add("DatasetSource-Source-OrbitPeriod");
		CategoricalVars.add("DatasetSource-Source-InclAngle");
		CategoricalVars.add("DatasetSource-Source-ShortName");
		CategoricalVars.add("DatasetSource-Sensor-SwathWidth");
		
		CategoricalVars.add("Collection-ShortName");
		CategoricalVars.add("DatasetCharacter-Value");
		CategoricalVars.add("DatasetCharacter-DatasetElement-Element-ShortName");
		
		//CategoricalVars.add("CollectionDataset-GranuleRange360");
		//CategoricalVars.add("DatasetPolicy-SpatialType");
		//CategoricalVars.add("DatasetCitation-SeriesName");
		//CategoricalVars.add("Dataset-ProviderDatasetName");
		//CategoricalVars.add("Dataset-AltitudeResolution");
		//CategoricalVars.add("Dataset-DepthResolution");
		//CategoricalVars.add("Dataset-DatasetCoverage-StopTimeLong");
		//CategoricalVars.add("Dataset-DatasetCoverage-StartTimeLong");
		//CategoricalVars.add("DatasetCoverage-StartTimeLong-Long");
		//CategoricalVars.add("DatasetCoverage-StopTimeLong-Long");
	}

	@Override
	public Object execute() {
		// TODO Auto-generated method stub
		this.OBEncodeVars();
		this.OBEncodeaAllMetadata();
		return null;
	}

	@Override
	public Object execute(Object o) {
		// TODO Auto-generated method stub
		return null;
	}
	
	private void OBEncodeaAllMetadata() {
		
		es.createBulkProcesser();
		
		/*SearchResponse scrollResp = es.client.prepareSearch(config.get("indexName")).setTypes(metadataType)
				.setQuery(QueryBuilders.matchAllQuery()).setSize(1).execute()
				.actionGet();
		
			for (SearchHit hit : scrollResp.getHits().getHits()) {
				Map<String, Object> metadata = hit.getSource();
				Map<String, Object> metadatacode = null;
				if(metadata !=null){
					try {
						metadatacode = OBEncodeMetadata(metadata);
						System.out.println(metadatacode.toString());
						JSONObject updateJson = new JSONObject(metadatacode);
						UpdateRequest ur = es.genUpdateRequest(config.get("indexName"), metadataType, hit.getId(), metadatacode);
						System.out.println(ur);
						System.out.println(es);
						System.out.println(es.bulkProcessor);
						es.bulkProcessor.add(ur);
						
					} catch (InterruptedException | ExecutionException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
				System.out.println(metadatacode);
			}*/

		SearchResponse scrollResp = es.client.prepareSearch(config.get("indexName")).setTypes(metadataType)
				.setScroll(new TimeValue(60000)).setQuery(QueryBuilders.matchAllQuery()).setSize(100).execute()
				.actionGet();
		while (true) {
			for (SearchHit hit : scrollResp.getHits().getHits()) {
				Map<String, Object> metadata = hit.getSource();
				Map<String, Object> metadatacode;
				try {
					metadatacode = OBEncodeMetadata(metadata);
					UpdateRequest ur = es.genUpdateRequest(config.get("indexName"), metadataType, hit.getId(), metadatacode);
					es.bulkProcessor.add(ur);
				} catch (InterruptedException | ExecutionException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}

			scrollResp = es.client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000))
					.execute().actionGet();
			if (scrollResp.getHits().getHits().length == 0) {
				break;
			}
		}
			
		es.destroyBulkProcessor();
	}
	
	private Map<String, Object> OBEncodeMetadata(Map<String, Object> metadata) throws InterruptedException, ExecutionException{
		String code = "";	
		Map<String, Object> metadataCodes = new HashMap<String, Object>();
		//System.out.println(metadata.get("Dataset-ShortName"));
		int CategoryNum = CategoricalVars.size();
		for(int i=0; i<CategoryNum; i++ ){
			String var = CategoricalVars.get(i);
			//System.out.println(var);
			Vector vec = null;
			if( metadata.get(var) != null && metadata.get(var) != ""){
				String value = es.customAnalyzing(config.get("indexName"), "csv", metadata.get(var).toString());
				//System.out.println(value);
				if(value.contains(",")){
					
					String[] values = value.split(",");
					int valuenum = values.length;
					Vector tmpvec = null;
					for(int j=0; j<valuenum; j++){
						tmpvec = getValueVec(var,values[j]);
						if(vec == null){
							vec = tmpvec;
						}else{
							vec = this.VectorSum(vec, tmpvec);
						}
					}
					//System.out.println(vec.toString());
				}else{
					vec = getValueVec(var,value);
					if(vec == null){
						 vec = getValueVec(var,VAR_NOT_EXIST);
					}
				}
			}else{
			    vec = getValueVec(var,VAR_NOT_EXIST);
			}	
			
			double[] codeArr = vec.toArray();
			String codeStr =  Arrays.toString(codeArr);
			codeStr = codeStr.substring(1, codeStr.length()-1);
			metadataCodes.put(var + "_code",  codeStr);
			code += codeStr + ",";
		}
		
		code = code.substring(0, code.length()-1);
		metadataCodes.put("Metadata_code",  code);
		return metadataCodes;
	}
	
	private Vector getValueVec(String var, String value){
		
		if(value.startsWith("[")){
			value = value.substring(1, value.length());
		}
		
		if(value.endsWith("]")){
			value = value.substring(0, value.length()-1);
		}
		
		Vector tmpvec = CategoricalVarValueVecs.get(var).get(value);
		if(tmpvec == null){
			tmpvec = CategoricalVarValueVecs.get(var).get(VAR_NOT_EXIST);
		}
		return tmpvec;
	}
	
	private void OBEncodeVars(){
		int CategoryNum = CategoricalVars.size();
		for(int i=0; i<CategoryNum; i++ ){
			String var = CategoricalVars.get(i);
			Map<String, Vector> valueVecs= this.OBEncodeVar(var);
			//System.out.println(var + " ： " + valueVecs.toString());
			CategoricalVarValueVecs.put(var, valueVecs);
		}
	}
	
	private Map<String, Vector> OBEncodeVar(String varName){
		
		SearchResponse sr = es.client.prepareSearch(config.get("indexName"))
			        .setTypes(config.get("recom_metadataType")).setQuery(QueryBuilders.matchAllQuery()).setSize(0)
			        .addAggregation(AggregationBuilders.terms("Values").field(varName).size(0))
			        .execute().actionGet();
	    Terms VarValues = sr.getAggregations().get("Values");
	    
	    Map<String, Vector> valueVec  = new HashMap<String, Vector>();
	    int valueNum = VarValues.getBuckets().size();
	    int pos =0; 
	    for (Terms.Bucket entry : VarValues.getBuckets()) {
	        String value = entry.getKey();
	        Vector sv = Vectors.sparse(valueNum, new int[] {pos}, new double[] {1});
	        pos+= 1; 
	        valueVec.put(value, sv);
	    }
	    
	    Vector sv = Vectors.sparse(valueNum, new int[] {0}, new double[] {0});
	    valueVec.put(VAR_NOT_EXIST, sv);

	    return valueVec;
	}

	private static Vector VectorSum(Vector v1, Vector v2) {
		double[] arr1 = v1.toArray();
		double[] arr2 = v2.toArray();
		
		int length = arr1.length;
		double[] arr3 = new double[length];
		for(int i=0; i<length; i++){
			 double value = arr1[i] + arr2[i];
			 if(value > 1.0){
				 value = 1.0;
			 }
			 arr3[i] = value;
		}
		
		Vector v3 = Vectors.dense(arr3);
		return v3;
	}
}
