package esiptestbed.mudrod.recommendation.process;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.spark.mllib.linalg.Vector;
import esiptestbed.mudrod.discoveryengine.DiscoveryStepAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.recommendation.structure.OHCodeExtractor;
import esiptestbed.mudrod.semantics.SemanticAnalyzer;
import esiptestbed.mudrod.utils.LinkageTriple;

public class ContentBasedCF extends DiscoveryStepAbstract implements Serializable {

	public ContentBasedCF(Map<String, String> config, ESDriver es,
		      SparkDriver spark) {
		// TODO Auto-generated constructor stub
		super(config, es, spark);
	}

	public Object execute1() {
		// TODO Auto-generated method stub
		SemanticAnalyzer analyzer = new SemanticAnalyzer(config, es, spark);
		String MatrixCodeFileName = config.get("metadataOBCodeMatrix");
		List<LinkageTriple> triples = analyzer.CalTermSimfromMatrix(MatrixCodeFileName, 0);
		
		List<LinkageTriple> lefttriples = this.filterTriples(triples);
		analyzer.SaveToES(lefttriples, config.get("indexName"), config.get("metadataCodeSimType"));

		return null;
	}
	
	@Override
	public Object execute() {
		// TODO Auto-generated method stub
		try {
			SemanticAnalyzer analyzer = new SemanticAnalyzer(config, es, spark);
			String MatrixCodeFileName = config.get("metadataOBCodeMatrix");
			List<LinkageTriple> triples = analyzer.CalTermSimfromMatrix(MatrixCodeFileName, 0);
			
			List<LinkageTriple> lefttriples = this.filterTriples(triples);
			analyzer.SaveToES(lefttriples, config.get("indexName"), config.get("metadataCodeSimType"), true);

			LinkageTriple.standardTriples(es, config.get("indexName"), config.get("metadataCodeSimType"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Object execute(Object o) {
		// TODO Auto-generated method stub
		return null;
	}
	
	private List<LinkageTriple> filterTriples(List<LinkageTriple> triples){

		OHCodeExtractor extractor = new OHCodeExtractor(config);
		List<String> fields = new ArrayList<String>();
		fields.add("DatasetParameter-Term");
		Map<String, Vector> metadataCode = extractor.loadFieldsOHEncode(es, fields);
		
		List<LinkageTriple> newtriples = new ArrayList<LinkageTriple>();
		int tripleSize = triples.size();
		for(int i=0; i<tripleSize; i++){
			LinkageTriple triple = triples.get(i);
			String keyA = triple.keyA;
			String keyB = triple.keyB;
			
			Vector vecA = metadataCode.get(keyA);
			Vector vecB = metadataCode.get(keyB);

			double product = this.dotProduct(vecA, vecB);
			if(product > 0.0 && !keyA.equals(keyB)){
				newtriples.add(triple);
			}
		}
		return newtriples;
	}
	
	private double dotProduct(Vector vecA, Vector vecB){
		double product = 0.0;
		
		double[] arrA = vecA.toDense().toArray();
		double[] arrB = vecB.toDense().toArray();
		
		int length = arrA.length;
		for(int i=0; i<length; i++){
			product += arrA[i] * arrB[i];
		}
			
		return product;
	}

}
