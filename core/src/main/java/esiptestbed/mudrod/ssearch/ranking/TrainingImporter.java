package esiptestbed.mudrod.ssearch.ranking;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import esiptestbed.mudrod.discoveryengine.MudrodAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.main.MudrodEngine;

public class TrainingImporter extends MudrodAbstract {

  public TrainingImporter(Map<String, String> config, ESDriver es,
      SparkDriver spark) {
    super(config, es, spark);
    es.deleteAllByQuery(config.get("indexName"),
        "trainingranking", QueryBuilders.matchAllQuery());
    addMapping();
  }
  
  public void addMapping() {
    XContentBuilder Mapping;
    try {
      Mapping = jsonBuilder().startObject()
          .startObject("trainingranking")
          .startObject("properties")
          .startObject("query")
          .field("type", "string").field("index", "not_analyzed")
          .endObject()
          .startObject("dataID")
          .field("type", "string").field("index", "not_analyzed")
          .endObject()
          .startObject("label").field("type", "string")
          .field("index", "not_analyzed")
          .endObject()
          .endObject().endObject().endObject();

      es.client.admin().indices().preparePutMapping(config.get("indexName"))
          .setType("trainingranking").setSource(Mapping)
          .execute().actionGet();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  public void importTrainingSet() throws IOException
  {
    es.createBulkProcesser();
    
    File[] files = new File("C:/mudrodCoreTestData/rankingResults/NewEvaluation/New folder/training").listFiles();
    for (File file : files) {
      BufferedReader br = new BufferedReader(new FileReader(file.getAbsolutePath()));
      br.readLine();
      String line = br.readLine();    
      while (line != null) {  
        String[] list = line.split(",");
        String query = file.getName().replace(".csv", "");
        if(list.length>0)
        {
        IndexRequest ir = new IndexRequest(config.get("indexName"),
            "trainingranking")
                .source(
                    jsonBuilder().startObject()
                        .field("query", query)
                        .field("dataID", list[0])
                        .field("label", list[list.length-1])
                        .endObject());
        es.bulkProcessor.add(ir);
        }
        line = br.readLine();
      }
      br.close();
    }
    es.destroyBulkProcessor();
  }


  public static void main(String[] args) throws IOException {
    // TODO Auto-generated method stub
    MudrodEngine mudrod = new MudrodEngine("Elasticsearch");
    TrainingImporter ti = new TrainingImporter(mudrod.getConfig(), mudrod.getES(), null);
    ti.importTrainingSet();
    mudrod.end();   
  }

}
