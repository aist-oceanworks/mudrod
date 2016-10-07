package esiptestbed.mudrod.weblog.structure;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.main.MudrodConstants;

public class LogTool implements Serializable {

  public LogTool() {

  }

  public List<String> getAllUsers(ESDriver es, Properties props,
      String httpType) {

    String indexName = props.getProperty(MudrodConstants.ES_INDEX_NAME);
    int docCount = es.getDocCount(indexName, httpType);

    SearchResponse sr = es.getClient().prepareSearch(indexName)
        .setTypes(httpType).setQuery(QueryBuilders.matchAllQuery()).setSize(0)
        .addAggregation(
            AggregationBuilders.terms("Users").field("IP").size(docCount))
        .execute().actionGet();
    Terms users = sr.getAggregations().get("Users");
    List<String> userList = new ArrayList<String>();
    for (Terms.Bucket entry : users.getBuckets()) {
      String ip = (String) entry.getKey();
      userList.add(ip);
    }

    return userList;
  }

  public List<String> getAllSessions(ESDriver es, Properties props,
      String cleanupLogType) {

    String indexName = props.getProperty(MudrodConstants.ES_INDEX_NAME);
    int docCount = es.getDocCount(indexName, cleanupLogType);

    SearchResponse sr = es.getClient()
        .prepareSearch(props.getProperty("indexName")).setTypes(cleanupLogType)
        .setQuery(QueryBuilders.matchAllQuery())
        .addAggregation(AggregationBuilders.terms("Sessions").field("SessionID")
            .size(docCount))
        .execute().actionGet();

    Terms Sessions = sr.getAggregations().get("Sessions");
    List<String> sessionList = new ArrayList<String>();
    for (Terms.Bucket entry : Sessions.getBuckets()) {
      if (entry.getDocCount() >= 3 && !entry.getKey().equals("invalid")) {
        String session = (String) entry.getKey();
        sessionList.add(session);
      }
    }

    return sessionList;
  }
}
