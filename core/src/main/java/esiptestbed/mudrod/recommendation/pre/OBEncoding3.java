package esiptestbed.mudrod.recommendation.pre;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;

import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;
import static org.apache.spark.sql.types.DataTypes.*;


// one binary encoding of metadata parameters
public class OBEncoding3 extends DiscoveryStepAbstract{

	private List<String> CategoricalVars;
	private Map<String, Map<String, Vector>> CategoricalVarValueVecs;
	private String metadataType;
	private String VAR_NOT_EXIST = "varNotExist";

	private StructType CategoricalVarschema;
	
	public OBEncoding3(Map<String, String> config, ESDriver es, SparkDriver spark) {
		super(config, es, spark);
		
		metadataType = config.get("recom_metadataType");
		CategoricalVarValueVecs = new HashMap<String, Map<String, Vector>>();
		CategoricalVars = new ArrayList<String>();
		CategoricalVars.add("Dataset-GridSpatialResolution");
		CategoricalVars.add("Dataset-SatelliteSpatialResolution");
		
		CategoricalVarschema = createStructType(new StructField[] {
				  createStructField("shortname", StringType, false),
				  createStructField("Dataset-GridSpatialResolution", new VectorUDT(), false),
				  createStructField("Dataset-SatelliteSpatialResolution", new VectorUDT(), false)
				});
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
				.setQuery(QueryBuilders.matchAllQuery()).setSize(5).execute()
				.actionGet();
		
			for (SearchHit hit : scrollResp.getHits().getHits()) {
				Map<String, Object> metadata = hit.getSource();
				String code = "";
				if(metadata !=null){
					code = OBEncodeMetadata(metadata);
				}
				
				System.out.println(code);
			}

		/*SearchResponse scrollResp = es.client.prepareSearch(config.get("indexName")).setTypes(metadataType)
				.setScroll(new TimeValue(60000)).setQuery(QueryBuilders.matchAllQuery()).setSize(100).execute()
				.actionGet();
		while (true) {
			for (SearchHit hit : scrollResp.getHits().getHits()) {
				Map<String, Object> metadata = hit.getSource();
				String code = OBEncodeMetadata(metadata);
				
				try {
					UpdateRequest ur;
					ur = new UpdateRequest(config.get("indexName"), metadataType, hit.getId())
					        .doc(jsonBuilder().startObject().field("code", code).endObject());
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
	
	private String OBEncodeMetadata(Map<String, Object> metadata){
		String code = "";
		
		int CategoryNum = CategoricalVars.size();
		Vector[] vecs = new Vector[CategoryNum];
		for(int i=0; i<CategoryNum; i++ ){
			String var = CategoricalVars.get(i);
			if( metadata.get(var) != null){
				String value = metadata.get(var).toString();
				System.out.println(value);
				Vector vec = CategoricalVarValueVecs.get(var).get(value);
				vecs[i] = vec.toDense();
			}else{
				System.out.println(VAR_NOT_EXIST);
				Vector vec = CategoricalVarValueVecs.get(var).get(VAR_NOT_EXIST);
				vecs[i] = vec.toDense();
			}	
		}
		
		Row row = RowFactory.create(metadata.get("Dataset-ShortName"), Arrays.asList(vecs));
		System.out.println(row.toString());
		
		JavaRDD<Row> rdd = spark.sc.parallelize(Arrays.asList(row));
		DataFrame dataset = spark.sqlContext.createDataFrame(rdd, CategoricalVarschema);

		VectorAssembler assembler = new VectorAssembler()
		  .setInputCols(CategoricalVars.toArray(new String[0]))
		  .setOutputCol("features");
		
		DataFrame output = assembler.transform(dataset);
		//System.out.println(output.select("features").first());
		
		return code;
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

}
