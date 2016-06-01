package esiptestbed.mudrod.utils;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import org.elasticsearch.action.index.IndexRequest;
import esiptestbed.mudrod.driver.ESDriver;

public class LinkageTriple implements Serializable {

	public long keyAId;
	public long keyBId;
	public double weight;
	public String keyA;
	public String keyB;
	
	public LinkageTriple() {
		// TODO Auto-generated constructor stub
	}
	
	public String toString(){
		return keyA + "," + keyB + ":" + weight;
	}
	
	public static void insertTriples(ESDriver es, List<LinkageTriple> triples,String index, String type) throws IOException{
		
		es.deleteType(index, type);
		
		es.createBulkProcesser();
		int size = triples.size();
		for(int i=0; i<size;i++){
			IndexRequest ir = new IndexRequest(index, type).source(jsonBuilder()
					.startObject()
					.field("keywords", triples.get(i).keyA + "," + triples.get(i).keyB)
					.field("weight", triples.get(i).weight)	
					.endObject());
			es.bulkProcessor.add(ir);
		}
		es.destroyBulkProcessor();
	}
}
