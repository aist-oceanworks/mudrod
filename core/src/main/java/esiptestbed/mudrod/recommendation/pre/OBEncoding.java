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
		//CategoricalVars.add("Dataset-GridSpatialResolution");
		//CategoricalVars.add("Dataset-SatelliteSpatialResolution");
		CategoricalVars.add("DatasetResource-Type");
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
		
		SearchResponse scrollResp = es.client.prepareSearch(config.get("indexName")).setTypes(metadataType)
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
						es.bulkProcessor.add(ur);
						
					} catch (InterruptedException | ExecutionException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
				System.out.println(metadatacode);
			}

		/*SearchResponse scrollResp = es.client.prepareSearch(config.get("indexName")).setTypes(metadataType)
				.setScroll(new TimeValue(60000)).setQuery(QueryBuilders.matchAllQuery()).setSize(100).execute()
				.actionGet();
		while (true) {
			for (SearchHit hit : scrollResp.getHits().getHits()) {
				Map<String, Object> metadata = hit.getSource();
				Map<String, Object> metadatacode = OBEncodeMetadata(metadata);
				
				try {
					UpdateRequest ur = es.genUpdateRequest(config.get("indexName"), metadataType, hit.getId(), metadatacode);
					es.bulkProcessor.add(ur);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			scrollResp = es.client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000))
					.execute().actionGet();
			if (scrollResp.getHits().getHits().length == 0) {
				break;
			}
		}*/
	}
	
	private Map<String, Object> OBEncodeMetadata(Map<String, Object> metadata) throws InterruptedException, ExecutionException{
		String code = "";
		//double[] codes = null;	
		Map<String, Object> metadataCodes = new HashMap<String, Object>();
		System.out.println(metadata.get("Dataset-ShortName"));
		int CategoryNum = CategoricalVars.size();
		for(int i=0; i<CategoryNum; i++ ){
			String var = CategoricalVars.get(i);
			//System.out.println(var);
			Vector vec = null;
			if( metadata.get(var) != null){
				String value = es.customAnalyzing(config.get("indexName"), "csv", metadata.get(var).toString());
				value = value.substring(1, value.length()-1);
				//System.out.println(value);
				if(value.contains(",")){
					String[] values = value.split(",");
					int valuenum = values.length;
					Vector tmpvec = null;
					for(int j=0; j<valuenum; j++){
						tmpvec = CategoricalVarValueVecs.get(var).get(values[j]);
						//System.out.println(values[j]);
						//System.out.println(tmpvec);
						if(vec == null){
							vec = tmpvec;
						}else{
							vec = this.VectorSum(vec, tmpvec);
						}
						//System.out.println(vec.toString());
					}
					
					System.out.println(vec.toString());
				}else{
					//System.out.println(value);
					vec = CategoricalVarValueVecs.get(var).get(value);	
				}
			}else{
				//System.out.println(VAR_NOT_EXIST);
			    vec = CategoricalVarValueVecs.get(var).get(VAR_NOT_EXIST);
			}	
			
			double[] codeArr = vec.toArray();
			String codeStr =  Arrays.toString(codeArr);
			codeStr = codeStr.substring(1, codeStr.length()-1);
			metadataCodes.put(var + "_code",  codeStr);
			//codes = (double[])ArrayUtils.addAll(codeArr, codes);
			
			code += codeStr + ",";
		}
		
		//code = Arrays.toString(codes);
		code = code.substring(0, code.length()-1);
		metadataCodes.put("metadata_code",  code);
		return metadataCodes;
	}
	
	private void OBEncodeVars(){
		int CategoryNum = CategoricalVars.size();
		for(int i=0; i<CategoryNum; i++ ){
			String var = CategoricalVars.get(i);
			Map<String, Vector> valueVecs= this.OBEncodeVar(var);
			System.out.println(valueVecs.toString());
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
