package esiptestbed.mudrod.ssearch.ranking;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Properties;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.main.MudrodEngine;
import esiptestbed.mudrod.ssearch.Searcher;
import esiptestbed.mudrod.ssearch.structure.SResult;

public class AutoTrainer {
  DecimalFormat NDForm = new DecimalFormat("#.###");
  
/*  public AutoTrainer(String fileName, String outputFilename) throws IOException { 
    File file = new File(outputFilename);
    if (file.exists()) {
      file.delete();
    }
    file.createNewFile();
    FileWriter fw = new FileWriter(outputFilename);
    BufferedWriter bw = new BufferedWriter(fw);

    MudrodEngine me = new MudrodEngine();
    Properties props = me.loadConfig();
    me.setESDriver(new ESDriver(props));
    me.setSparkDriver(new SparkDriver());

    Searcher sr = new Searcher(me.getConfig(), me.getESDriver(), null);

    BufferedReader br = new BufferedReader(new FileReader(fileName));
    String line = br.readLine();
    int count = 0;
    String output = "";
    while (line != null) { 
      String ele = line.substring(0, line.length()-1);
      JsonElement jelement = new JsonParser().parse(ele);
      JsonObject  job = jelement.getAsJsonObject();
      String query = job.get("query").toString().replace("\"", "");
      String data_good = job.get("highRankDataset").toString().replace("\"", "");
      String data_bad = job.get("lowRankDataset").toString().replace("\"", ""); 

      SResult result = null;
      if((count & 1) == 0)
      { 
        output = "0 ";
        result = sr.getAutoPair(me.getConfig().getProperty("indexName"), 
            me.getConfig().getProperty("raw_metadataType"), query, "phrase", data_bad, data_good);
      }
      else{
        output = "1 ";
        result = sr.getAutoPair(me.getConfig().getProperty("indexName"), 
            me.getConfig().getProperty("raw_metadataType"), query, "phrase", data_good, data_bad);
      }

      if(result!=null)
      {
        String[] resutl_str = result.toString(",").split(",");
        for(int i=0; i < resutl_str.length; i++)
        {
          int index = i + 1;
          output += index + ":" + NDForm.format(Double.parseDouble(resutl_str[i].replace("\"", ""))) + " ";
        }
        bw.write(output + "\n");
      }

      line = br.readLine();
      count++;
    }
    br.close();
    bw.close();
  }*/
 
  String[] filterList = {"measurement", "variable", "sensor", "platform"};
  public AutoTrainer(String fileName, String outputFilename) throws IOException { 
    File file = new File(outputFilename);
    if (file.exists()) {
      file.delete();
    }
    file.createNewFile();
    FileWriter fw = new FileWriter(outputFilename);
    BufferedWriter bw = new BufferedWriter(fw);

    MudrodEngine me = new MudrodEngine();
    Properties props = me.loadConfig();
    me.setESDriver(new ESDriver(props));
    me.setSparkDriver(new SparkDriver());

    Searcher sr = new Searcher(me.getConfig(), me.getESDriver(), null);

    BufferedReader br = new BufferedReader(new FileReader(fileName));
    String line = br.readLine();
    int count = 0;
    String output = "";
    bw.write(SResult.getHeader(",") + "label\n");
    while (line != null) { 
      String ele = line.substring(0, line.length()-1);
      JsonElement jelement = new JsonParser().parse(ele);
      JsonObject  job = jelement.getAsJsonObject();
      String query = job.get("query").toString().replace("\"", "");
      String data_good = job.get("highRankDataset").toString().replace("\"", "");
      String data_bad = job.get("lowRankDataset").toString().replace("\"", ""); 

      SResult result = null;
      if((count & 1) == 0)
      { 
        output = "0 ";
        result = sr.getAutoPair(me.getConfig().getProperty("indexName"), 
            me.getConfig().getProperty("raw_metadataType"), query, "phrase", data_bad, data_good);
        if(result!=null)
        {
          bw.write(result.toString(",") + "-1,\n");
        }
        
//        for(String str:filterList)
//        {
//          if(!str.equals(query))
//          result = sr.getAutoPair(me.getConfig().getProperty("indexName"), 
//              me.getConfig().getProperty("raw_metadataType"), str, "phrase", data_bad, data_good);
//          if(result!=null)
//          {
//            bw.write(result.toString(",") + "-1,\n");
//          }
//        }
      }
      else{
        output = "1 ";
        result = sr.getAutoPair(me.getConfig().getProperty("indexName"), 
            me.getConfig().getProperty("raw_metadataType"), query, "phrase", data_good, data_bad);
        if(result!=null)
        {
          bw.write(result.toString(",") + "1,\n");
        }
        
//        for(String str:filterList)
//        {
//          if(!str.equals(query))
//          result = sr.getAutoPair(me.getConfig().getProperty("indexName"), 
//              me.getConfig().getProperty("raw_metadataType"), str, "phrase", data_bad, data_good);
//          if(result!=null)
//          {
//            bw.write(result.toString(",") + "-1,\n");
//          }
//        }
      }

      line = br.readLine();
      count++;
    }
    br.close();
    bw.close();
  }


  public static void main(String[] args) throws IOException {
    AutoTrainer at = new AutoTrainer("C:/mudrodCoreTestData/rankingResults/training/autoData",
        "C:/mudrodCoreTestData/rankingResults/training_auto_new_only_query.csv");
  }

}
