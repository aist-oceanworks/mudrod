/*
 * Licensed under the Apache License, Version 2.0 (the "License"); you 
 * may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gov.nasa.jpl.mudrod.driver;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import gov.nasa.jpl.mudrod.main.MudrodConstants;
import gov.nasa.jpl.mudrod.main.MudrodEngine;
import gov.nasa.jpl.mudrod.utils.ESTransportClient;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse.AnalyzeToken;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Driver implementation for all Elasticsearch functionality.
 */
public class ESDriver implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(ESDriver.class);
  private static final long serialVersionUID = 1L;
  private transient Client client = null;
  private transient Node node = null;
  private transient BulkProcessor bulkProcessor = null;
  private static final String DS_PARAM_VAR = "DatasetParameter-Variable";

  /**
   * Default constructor for this class. To load client configuration call
   * substantiated constructor.
   */
  public ESDriver() {
    // Default constructor, to load configuration call ESDriver(props)
  }

  /**
   * Substantiated constructor which accepts a {@link java.util.Properties}
   *
   * @param props a populated properties object.
   */
  public ESDriver(Properties props) {
    try {
      setClient(makeClient(props));
    } catch (IOException e) {
      LOG.error("Error whilst constructing Elastcisearch client.", e);
    }
  }

  public void createBulkProcessor() {
    LOG.debug("Creating BulkProcessor with maxBulkDocs={}, maxBulkLength={}", 1000, 2500500);
    setBulkProcessor(BulkProcessor.builder(getClient(), new BulkProcessor.Listener() {
      @Override
      public void beforeBulk(long executionId, BulkRequest request) {
        LOG.debug("ESDriver#createBulkProcessor @Override #beforeBulk is not implemented yet!");
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
        LOG.debug("ESDriver#createBulkProcessor @Override #afterBulk is not implemented yet!");
      }

      @Override
      public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
        LOG.error("Bulk request has failed!");
        throw new RuntimeException("Caught exception in bulk: " + request.getDescription() + ", failure: " + failure, failure);
      }
    }).setBulkActions(1000).setBulkSize(new ByteSizeValue(2500500, ByteSizeUnit.GB)).setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 10)).setConcurrentRequests(1)
        .build());
  }

  public void destroyBulkProcessor() {
    try {
      getBulkProcessor().awaitClose(20, TimeUnit.MINUTES);
      setBulkProcessor(null);
      refreshIndex();
    } catch (InterruptedException e) {
      LOG.error("Error destroying the Bulk Processor.", e);
    }
  }

  public void putMapping(String indexName, String settingsJson, String mappingJson) throws IOException {

    boolean exists = getClient().admin().indices().prepareExists(indexName).execute().actionGet().isExists();
    if (exists) {
      return;
    }

    getClient().admin().indices().prepareCreate(indexName).setSettings(Settings.builder().loadFromSource(settingsJson)).execute().actionGet();
    getClient().admin().indices().preparePutMapping(indexName).setType("_default_").setSource(mappingJson).execute().actionGet();
  }

  public String customAnalyzing(String indexName, String str) throws InterruptedException, ExecutionException {
    return this.customAnalyzing(indexName, "cody", str);
  }

  public String customAnalyzing(String indexName, String analyzer, String str) throws InterruptedException, ExecutionException {
    String[] strList = str.toLowerCase().split(",");
    for (int i = 0; i < strList.length; i++) {
      String tmp = "";
      AnalyzeResponse r = client.admin().indices().prepareAnalyze(strList[i]).setIndex(indexName).setAnalyzer(analyzer).execute().get();
      for (AnalyzeToken token : r.getTokens()) {
        tmp += token.getTerm() + " ";
      }
      strList[i] = tmp.trim();
    }
    return String.join(",", strList);
  }

  public List<String> customAnalyzing(String indexName, List<String> list) throws InterruptedException, ExecutionException {
    if (list == null) {
      return list;
    }
    int size = list.size();
    List<String> customlist = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      customlist.add(this.customAnalyzing(indexName, list.get(i)));
    }

    return customlist;
  }

  public void deleteAllByQuery(String index, String type, QueryBuilder query) {
    createBulkProcessor();
    SearchResponse scrollResp = getClient().prepareSearch(index).setSearchType(SearchType.QUERY_AND_FETCH).setTypes(type).setScroll(new TimeValue(60000)).setQuery(query).setSize(10000).execute()
        .actionGet();

    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        DeleteRequest deleteRequest = new DeleteRequest(index, type, hit.getId());
        getBulkProcessor().add(deleteRequest);
      }

      scrollResp = getClient().prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }

    }
    destroyBulkProcessor();
  }

  public void deleteType(String index, String type) {
    this.deleteAllByQuery(index, type, QueryBuilders.matchAllQuery());
  }

  public List<String> getTypeListWithPrefix(Object object, Object object2) {
    ArrayList<String> typeList = new ArrayList<>();
    GetMappingsResponse res;
    try {
      res = getClient().admin().indices().getMappings(new GetMappingsRequest().indices(object.toString())).get();
      ImmutableOpenMap<String, MappingMetaData> mapping = res.mappings().get(object.toString());
      for (ObjectObjectCursor<String, MappingMetaData> c : mapping) {
        if (c.key.startsWith(object2.toString())) {
          typeList.add(c.key);
        }
      }
    } catch (InterruptedException | ExecutionException e) {
      LOG.error("Error whilst obtaining type list from Elasticsearch mappings.", e);
    }
    return typeList;
  }

  public List<String> getIndexListWithPrefix(Object object) {

    LOG.info("Retrieving index list with prefix: {}", object.toString());
    String[] indices = client.admin().indices().getIndex(new GetIndexRequest()).actionGet().getIndices();

    ArrayList<String> indexList = new ArrayList<>();
    int length = indices.length;
    for (int i = 0; i < length; i++) {
      String indexName = indices[i];
      if (indexName.startsWith(object.toString())) {
        indexList.add(indexName);
      }
    }

    return indexList;
  }

  public String searchByQuery(String index, String type, String query) throws IOException, InterruptedException, ExecutionException {
    return searchByQuery(index, type, query, false);
  }

  public String searchByQuery(String index, String type, String query, Boolean bDetail) throws IOException, InterruptedException, ExecutionException {
    boolean exists = getClient().admin().indices().prepareExists(index).execute().actionGet().isExists();
    if (!exists) {
      return null;
    }

    QueryBuilder qb = QueryBuilders.queryStringQuery(query);
    SearchResponse response = getClient().prepareSearch(index).setTypes(type).setQuery(qb).setSize(500).execute().actionGet();

    Gson gson = new Gson();
    List<JsonObject> fileList = new ArrayList<>();

    for (SearchHit hit : response.getHits().getHits()) {
      Map<String, Object> result = hit.getSource();
      String shortName = (String) result.get("Dataset-ShortName");
      String longName = (String) result.get("Dataset-LongName");
      ArrayList<String> topicList = (ArrayList<String>) result.get(DS_PARAM_VAR);
      String topic = String.join(", ", topicList);
      String content = (String) result.get("Dataset-Description");

      ArrayList<String> longdate = (ArrayList<String>) result.get("DatasetCitation-ReleaseDateLong");

      Date date = new Date(Long.parseLong(longdate.get(0)));
      SimpleDateFormat df2 = new SimpleDateFormat("dd/MM/yyyy");
      String dateText = df2.format(date);

      JsonObject file = new JsonObject();
      file.addProperty("Short Name", shortName);
      file.addProperty("Long Name", longName);
      file.addProperty("Topic", topic);
      file.addProperty("Dataset-Description", content);
      file.addProperty("Release Date", dateText);

      if (bDetail) {    	  
      String dataFormat = (String) result.get("DatasetPolicy-DataFormat");
      if("RAW".equals(dataFormat)){
        dataFormat = "BINARY";
      }
    	file.addProperty("DataFormat", dataFormat);  
    	file.addProperty("Dataset-Doi", (String) result.get("Dataset-Doi"));
        file.addProperty("Processing Level",
            (String) result.get("Dataset-ProcessingLevel"));
        
        List<String> versions = (List<String>) result
                .get("DatasetCitation-Version");
        file.addProperty("Version",  String.join(", ", versions));

        List<String> sensors = (List<String>) result
            .get("DatasetSource-Sensor-ShortName");
        file.addProperty("DatasetSource-Sensor-ShortName",
            String.join(", ", sensors));

        List<String> projects = (List<String>) result
            .get("DatasetProject-Project-ShortName");
        file.addProperty("DatasetProject-Project-ShortName",
            String.join(", ", projects));

        List<String> categories = (List<String>) result
            .get("DatasetParameter-Category");
        file.addProperty("DatasetParameter-Category",
            String.join(", ", categories));
        
        List<String> urls = (List<String>) result
            .get("DatasetLocationPolicy-BasePath");
        
        List<String> filtered_urls = new ArrayList<String>();
        for(String url:urls)
        {
          if(url.startsWith("ftp")||url.startsWith("http"))
            filtered_urls.add(url);
        }
        file.addProperty("DatasetLocationPolicy-BasePath",
            String.join(",", filtered_urls));

        List<String> variables = (List<String>) result
            .get(DS_PARAM_VAR);

        file.addProperty(DS_PARAM_VAR, String.join(", ", variables));

        List<String> terms = (List<String>) result.get("DatasetParameter-Term");
        file.addProperty("DatasetParameter-Term", String.join(", ", terms));

        /********coverage*********/

        List<String> region = (List<String>) result.get("DatasetRegion-Region");
        file.addProperty("Region", String.join(", ", region));

        file.addProperty("NorthLat", (String) result.get("DatasetCoverage-NorthLat"));
        file.addProperty("SouthLat", (String) result.get("DatasetCoverage-SouthLat"));
        file.addProperty("WestLon", (String) result.get("DatasetCoverage-WestLon"));
        file.addProperty("EastLon", (String) result.get("DatasetCoverage-EastLon"));
            
        //start date
        Long start = (Long) result.get("DatasetCoverage-StartTimeLong-Long");
        Date startDate = new Date(start);
        String startDateTxt = df2.format(startDate);

        //end date
        String end = (String) result.get("Dataset-DatasetCoverage-StopTimeLong");
        String endDateTxt = "";
        if (end.equals("")) {
          endDateTxt = "Present";
        } else {
          Date endDate = new Date(Long.valueOf(end));
          endDateTxt = df2.format(endDate);
        }
        file.addProperty("Time Span", startDateTxt + " to " + endDateTxt);
        /********coverage*********/

        /********resolution*********/
        //temporal
        String temporalResolution = (String) result.get("Dataset-TemporalResolution");
        if ("".equals(temporalResolution)) {
          temporalResolution = (String) result.get("Dataset-TemporalRepeat");
        }
        file.addProperty("TemporalResolution", temporalResolution);

        //spatial
        String latResolution = (String) result.get("Dataset-LatitudeResolution");
        String lonResolution = (String) result.get("Dataset-LongitudeResolution");
        if (!latResolution.isEmpty() && !lonResolution.isEmpty()) {
          file.addProperty("SpatiallResolution", latResolution + " degrees (latitude) x " + lonResolution + " degrees (longitude)");
        } else {
          String acrossResolution = (String) result.get("Dataset-AcrossTrackResolution");
          String alonResolution = (String) result.get("Dataset-AlongTrackResolution");
          double dAcrossResolution = Double.parseDouble(acrossResolution)/1000;
          double dAlonResolution = Double.parseDouble(alonResolution)/1000;
          file.addProperty("SpatiallResolution", dAlonResolution + " km (Along) x " + dAcrossResolution + " km (Across)");
        }
      }

      fileList.add(file);

    }
    JsonElement fileListElement = gson.toJsonTree(fileList);

    JsonObject pdResults = new JsonObject();
    pdResults.add("PDResults", fileListElement);
    LOG.info("Search results returned. \n");
    return pdResults.toString();
  }

  public List<String> autoComplete(String index, String term) {
    boolean exists = this.getClient().admin().indices().prepareExists(index).execute().actionGet().isExists();
    if (!exists) {
      return new ArrayList<>();
    }

    Set<String> suggestHS = new HashSet<String>();
    List<String> suggestList = new ArrayList<>();

    // please make sure that the completion field is configured in the ES mapping
    CompletionSuggestionBuilder suggestionsBuilder = SuggestBuilders.completionSuggestion("Dataset-Metadata").prefix(term, Fuzziness.fromEdits(2)).size(100);
    SearchRequestBuilder suggestRequestBuilder = getClient().prepareSearch(index).suggest(new SuggestBuilder().addSuggestion("completeMe", suggestionsBuilder));
    SearchResponse sr = suggestRequestBuilder.setFetchSource(false).execute().actionGet();

    Iterator<? extends Suggest.Suggestion.Entry.Option> iterator = sr.getSuggest().getSuggestion("completeMe").iterator().next().getOptions().iterator();

    while (iterator.hasNext()) {
      Suggest.Suggestion.Entry.Option next = iterator.next();
      String suggest = next.getText().string().toLowerCase();
      suggestList.add(suggest);
    }

    suggestHS.addAll(suggestList);
    suggestList.clear();
    suggestList.addAll(suggestHS);
    return suggestList;
  }

  public void close() {
    client.close();
  }

  public void refreshIndex() {
    client.admin().indices().prepareRefresh().execute().actionGet();
  }

  /**
   * Generates a TransportClient or NodeClient
   *
   * @param props a populated {@link java.util.Properties} object
   * @return a constructed {@link org.elasticsearch.client.Client}
   * @throws IOException if there is an error building the
   *                     {@link org.elasticsearch.client.Client}
   */
  protected Client makeClient(Properties props) throws IOException {
    String clusterName = props.getProperty(MudrodConstants.ES_CLUSTER);
    String hostsString = props.getProperty(MudrodConstants.ES_UNICAST_HOSTS);
    String[] hosts = hostsString.split(",");
    String portStr = props.getProperty(MudrodConstants.ES_TRANSPORT_TCP_PORT);
    int port = Integer.parseInt(portStr);

    Settings.Builder settingsBuilder = Settings.builder();

    // Set the cluster name and build the settings
    if (!clusterName.isEmpty())
      settingsBuilder.put("cluster.name", clusterName);

    settingsBuilder.put("http.type", "netty3");
    settingsBuilder.put("transport.type", "netty3");

    Settings settings = settingsBuilder.build();

    Client client = null;

    // Prefer TransportClient
    if (hosts != null && port > 1) {
      TransportClient transportClient = new ESTransportClient(settings);
      for (String host : hosts)
        transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
      client = transportClient;
    } else if (clusterName != null) {
      node = new Node(settings);
      client = node.client();
    }

    return client;
  }

  /**
   * Main method used to invoke the ESDriver implementation.
   *
   * @param args no arguments are required to invoke the Driver.
   */
  public static void main(String[] args) {
    MudrodEngine mudrodEngine = new MudrodEngine();
    ESDriver es = new ESDriver(mudrodEngine.loadConfig());
    es.getTypeListWithPrefix("podaacsession", "sessionstats");
  }

  /**
   * @return the client
   */
  public Client getClient() {
    return client;
  }

  /**
   * @param client the client to set
   */
  public void setClient(Client client) {
    this.client = client;
  }

  /**
   * @return the bulkProcessor
   */
  public BulkProcessor getBulkProcessor() {
    return bulkProcessor;
  }

  /**
   * @param bulkProcessor the bulkProcessor to set
   */
  public void setBulkProcessor(BulkProcessor bulkProcessor) {
    this.bulkProcessor = bulkProcessor;
  }

  public UpdateRequest generateUpdateRequest(String index, String type, String id, String field1, Object value1) {

    UpdateRequest ur = null;
    try {
      ur = new UpdateRequest(index, type, id).doc(jsonBuilder().startObject().field(field1, value1).endObject());
    } catch (IOException e) {
      LOG.error("Error whilst attempting to generate a new Update Request.", e);
    }

    return ur;
  }

  public UpdateRequest generateUpdateRequest(String index, String type, String id, Map<String, Object> filedValueMap) {

    UpdateRequest ur = null;
    try {
      XContentBuilder builder = jsonBuilder().startObject();
      for (Entry<String, Object> entry : filedValueMap.entrySet()) {
        String key = entry.getKey();
        builder.field(key, filedValueMap.get(key));
      }
      builder.endObject();
      ur = new UpdateRequest(index, type, id).doc(builder);
    } catch (IOException e) {
      LOG.error("Error whilst attempting to generate a new Update Request.", e);
    }

    return ur;
  }

  public int getDocCount(String index, String... type) {
    MatchAllQueryBuilder search = QueryBuilders.matchAllQuery();
    String[] indexArr = new String[] { index };
    return this.getDocCount(indexArr, type, search);
  }

  public int getDocCount(String[] index, String[] type) {
    MatchAllQueryBuilder search = QueryBuilders.matchAllQuery();
    return this.getDocCount(index, type, search);
  }

  public int getDocCount(String[] index, String[] type, QueryBuilder filterSearch) {
    SearchRequestBuilder countSrBuilder = getClient().prepareSearch(index).setTypes(type).setQuery(filterSearch).setSize(0);
    SearchResponse countSr = countSrBuilder.execute().actionGet();
    int docCount = (int) countSr.getHits().getTotalHits();
    return docCount;
  }
}
