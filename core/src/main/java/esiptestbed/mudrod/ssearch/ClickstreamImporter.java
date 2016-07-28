package esiptestbed.mudrod.ssearch;

import java.util.Map;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import esiptestbed.mudrod.discoveryengine.MudrodAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;

public class ClickstreamImporter extends MudrodAbstract {

  public ClickstreamImporter(Map<String, String> config, ESDriver es, SparkDriver spark) {
    super(config, es, spark);
    // TODO Auto-generated constructor stub
    addClickStreamMapping();
  }

  public void addClickStreamMapping(){
    XContentBuilder Mapping;
    try {
      Mapping = jsonBuilder()
          .startObject()
          .startObject(config.get("clickstreamMatrixType"))
          .startObject("properties")
          .startObject("query")
          .field("type", "string")
          .field("index", "not_analyzed")
          .endObject()
          .startObject("dataID")
          .field("type", "string")
          .field("index", "not_analyzed")
          .endObject()

          .endObject()
          .endObject()
          .endObject();

      es.client.admin().indices()
      .preparePutMapping(config.get("indexName"))
      .setType(config.get("clickstreamMatrixType"))
      .setSource(Mapping)
      .execute().actionGet();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } 
  }

  public void importfromCSVtoES(){
    es.deleteType(config.get("indexName"), config.get("clickstreamMatrixType"));    
    es.createBulkProcesser();

    BufferedReader br = null;
    String cvsSplitBy = ",";

    try {
      br = new BufferedReader(new FileReader(config.get("clickstreamMatrix")));
      String line = br.readLine();
      String dataList[] = line.split(cvsSplitBy);  // first item need to be skipped
      while ((line = br.readLine()) != null) {
        String[] clicks = line.split(cvsSplitBy);
        for(int i=1; i<clicks.length; i++)
        {
          if(!clicks[i].equals("0.0"))
          {
            IndexRequest ir = new IndexRequest(config.get("indexName"), config.get("clickstreamMatrixType")).source(jsonBuilder()
                .startObject()
                .field("query", clicks[0])
                .field("dataID", dataList[i]) 
                .field("clicks", clicks[i])
                .endObject());
            es.bulkProcessor.add(ir);
          }
        }
      }   
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (br != null) {
        try {
          br.close();
          es.destroyBulkProcessor();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

}
