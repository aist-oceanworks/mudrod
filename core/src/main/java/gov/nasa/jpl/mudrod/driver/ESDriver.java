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
import com.google.gson.GsonBuilder;
import gov.nasa.jpl.mudrod.main.MudrodConstants;
import gov.nasa.jpl.mudrod.main.MudrodEngine;
import gov.nasa.jpl.mudrod.utils.ESTransportClient;
import org.apache.commons.lang.StringUtils;
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
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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

  @SuppressWarnings("unchecked")
  public String searchByQuery(String index, String type, String query, Boolean bDetail) throws IOException, InterruptedException, ExecutionException {
    boolean exists = getClient().admin().indices().prepareExists(index).execute().actionGet().isExists();
    if (!exists) {
      return null;
    }

    QueryBuilder qb = QueryBuilders.queryStringQuery(query);
    SearchResponse response = getClient().prepareSearch(index).setTypes(type).setQuery(qb).setSize(500).execute().actionGet();

    // Map of K,V pairs where key is the field name from search result and value is the that should be returned for that field. Not always the same.
    Map<String, String> fieldsToReturn = new HashMap<>();

    fieldsToReturn.put("Dataset-ShortName", "Short Name");
    fieldsToReturn.put("Dataset-LongName", "Long Name");
    fieldsToReturn.put("DatasetParameter-Topic", "Topic");
    fieldsToReturn.put("Dataset-Description", "Dataset-Description");
    fieldsToReturn.put("DatasetCitation-ReleaseDateLong", "Release Date");

    if (bDetail) {
      fieldsToReturn.put("DatasetPolicy-DataFormat", "DataFormat");
      fieldsToReturn.put("Dataset-Doi", "Dataset-Doi");
      fieldsToReturn.put("Dataset-ProcessingLevel", "Processing Level");
      fieldsToReturn.put("DatasetCitation-Version", "Version");
      fieldsToReturn.put("DatasetSource-Sensor-ShortName", "DatasetSource-Sensor-ShortName");
      fieldsToReturn.put("DatasetProject-Project-ShortName", "DatasetProject-Project-ShortName");
      fieldsToReturn.put("DatasetParameter-Category", "DatasetParameter-Category");
      fieldsToReturn.put("DatasetLocationPolicy-BasePath", "DatasetLocationPolicy-BasePath");
      fieldsToReturn.put("DatasetParameter-Variable-Full", "DatasetParameter-Variable-Full");
      fieldsToReturn.put("DatasetParameter-Term-Full", "DatasetParameter-Term-Full");
      fieldsToReturn.put("DatasetParameter-VariableDetail", "DatasetParameter-VariableDetail");

      fieldsToReturn.put("DatasetRegion-Region", "Region");
      fieldsToReturn.put("DatasetCoverage-NorthLat", "NorthLat");
      fieldsToReturn.put("DatasetCoverage-SouthLat", "SouthLat");
      fieldsToReturn.put("DatasetCoverage-WestLon", "WestLon");
      fieldsToReturn.put("DatasetCoverage-EastLon", "EastLon");
      fieldsToReturn.put("DatasetCoverage-StartTimeLong-Long", "DatasetCoverage-StartTimeLong-Long");
      fieldsToReturn.put("Dataset-DatasetCoverage-StopTimeLong", "Dataset-DatasetCoverage-StopTimeLong");

      fieldsToReturn.put("Dataset-TemporalResolution", "Dataset-TemporalResolution");
      fieldsToReturn.put("Dataset-TemporalRepeat", "Dataset-TemporalRepeat");
      fieldsToReturn.put("Dataset-LatitudeResolution", "Dataset-LatitudeResolution");
      fieldsToReturn.put("Dataset-LongitudeResolution", "Dataset-LongitudeResolution");
      fieldsToReturn.put("Dataset-AcrossTrackResolution", "Dataset-AcrossTrackResolution");
      fieldsToReturn.put("Dataset-AlongTrackResolution", "Dataset-AlongTrackResolution");
    }

    List<Map<String, Object>> searchResults = new ArrayList<>();

    for (SearchHit hit : response.getHits().getHits()) {
      Map<String, Object> source = hit.getSource();

      Map<String, Object> searchResult = source.entrySet().stream().filter(entry -> fieldsToReturn.keySet().contains(entry.getKey()))
          .collect(Collectors.toMap(entry -> fieldsToReturn.get(entry.getKey()), Entry::getValue));

      // searchResult is now a map where the key = value from fieldsToReturn and the value = value from search result

      // Some results require special handling/formatting:
      // Release Date formatting
      LocalDate releaseDate = Instant.ofEpochMilli(Long.parseLong(((ArrayList<String>) searchResult.get("Release Date")).get(0))).atZone(ZoneId.of("Z")).toLocalDate();
      searchResult.put("Release Date", releaseDate.format(DateTimeFormatter.ISO_DATE));

      if (bDetail) {

        // DataFormat value, translate RAW to BINARY
        if ("RAW".equals(searchResult.get("DataFormat"))) {
          searchResult.put("DataFormat", "BINARY");
        }

        // DatasetLocationPolicy-BasePath Should only contain ftp, http, or https URLs
        List<String> urls = ((List<String>) searchResult.get("DatasetLocationPolicy-BasePath")).stream().filter(url -> url.startsWith("ftp") || url.startsWith("http")).collect(Collectors.toList());
        searchResult.put("DatasetLocationPolicy-BasePath", urls);

        // Time Span Formatting
        LocalDate startDate = Instant.ofEpochMilli((Long) searchResult.get("DatasetCoverage-StartTimeLong-Long")).atZone(ZoneId.of("Z")).toLocalDate();
        LocalDate endDate = "".equals(searchResult.get("Dataset-DatasetCoverage-StopTimeLong")) ?
            null :
            Instant.ofEpochMilli(Long.parseLong(searchResult.get("Dataset-DatasetCoverage-StopTimeLong").toString())).atZone(ZoneId.of("Z")).toLocalDate();
        searchResult.put("Time Span", startDate.format(DateTimeFormatter.ISO_DATE) + " to " + (endDate == null ? "Present" : endDate.format(DateTimeFormatter.ISO_DATE)));

        // Temporal resolution can come from one of two fields
        searchResult.put("TemporalResolution", "".equals(searchResult.get("Dataset-TemporalResolution")) ? searchResult.get("Dataset-TemporalRepeat") : searchResult.get("Dataset-TemporalResolution"));

        // Special formatting for spatial resolution
        String latResolution = (String) searchResult.get("Dataset-LatitudeResolution");
        String lonResolution = (String) searchResult.get("Dataset-LongitudeResolution");
        if (!latResolution.isEmpty() && !lonResolution.isEmpty()) {
          searchResult.put("SpatialResolution", latResolution + " degrees (latitude) x " + lonResolution + " degrees (longitude)");
        } else {
          String acrossResolution = (String) searchResult.get("Dataset-AcrossTrackResolution");
          String alonResolution = (String) searchResult.get("Dataset-AlongTrackResolution");
          double dAcrossResolution = Double.parseDouble(acrossResolution) / 1000;
          double dAlonResolution = Double.parseDouble(alonResolution) / 1000;
          searchResult.put("SpatialResolution", dAlonResolution + " km (Along) x " + dAcrossResolution + " km (Across)");
        }

        // Measurement is a list of hierarchies that goes Topic -> Term -> Variable -> Variable Detail. Need to construct these hierarchies.
        List<List<String>> measurements = buildMeasurementHierarchies((List<String>) searchResult.get("Topic"), (List<String>) searchResult.get("DatasetParameter-Term-Full"),
            (List<String>) searchResult.get("DatasetParameter-Variable-Full"), (List<String>) searchResult.get("DatasetParameter-VariableDetail"));

        searchResult.put("Measurements", measurements);

      }

      searchResults.add(searchResult);
    }

    Map<String, List<?>> pdResults = new HashMap<>();
    pdResults.put("PDResults", searchResults);

    return new GsonBuilder().create().toJson(pdResults);
  }

  /**
   * Builds a List of Measurement Hierarchies given the individual source lists.
   * The hierarchy is built from the element in the same position from each input list in the order: Topic -> Term -> Variable -> VariableDetail
   * "None" and blank strings are ignored. If, at any level, an element does not exist for that position or it is "None" or blank, that hierarchy is considered complete.
   *
   * For example, if the input is:
   * <pre>
   * topics = ["Oceans", "Oceans"]
   * terms = ["Sea Surface Topography", "Ocean Waves"]
   * variables = ["Sea Surface Height", "Significant Wave Height"]
   * variableDetails = ["None", "None"]
   * </pre>
   *
   * The output would be:
   * <pre>
   *   [
   *     ["Oceans", "Sea Surface Topography", "Sea Surface Height"],
   *     ["Oceans", "Ocean Waves", "Significant Wave Height"]
   *   ]
   * </pre>
   *     Oceans > Sea Surface Topography > Sea Surface Height
   *     Oceans > Ocean Waves > Significant Wave Height
   *
   * @param topics List of topics, the first element of a measurement
   * @param terms List of terms, the second element of a measurement
   * @param variables List of variables, the third element of a measurement
   * @param variableDetails List of variable details, the fourth element of a measurement
   *
   * @return A List where each element is a single hierarchy (as a List) built from the provided input lists.
   */
  private List<List<String>> buildMeasurementHierarchies(List<String> topics, List<String> terms, List<String> variables, List<String> variableDetails) {

    List<List<String>> measurements = new ArrayList<>();

    for (int x = 0; x < topics.size(); x++) {
      measurements.add(new ArrayList<>());
      measurements.get(x).add(topics.get(x));
      // Only add the next 'level' if we can
      if (x < terms.size() && !"None".equalsIgnoreCase(terms.get(x)) && StringUtils.isNotBlank(terms.get(x))) {
        measurements.get(x).add(terms.get(x));
        if (x < variables.size() && !"None".equalsIgnoreCase(variables.get(x)) && StringUtils.isNotBlank(variables.get(x))) {
          measurements.get(x).add(variables.get(x));
          if (x < variableDetails.size() && !"None".equalsIgnoreCase(variableDetails.get(x)) && StringUtils.isNotBlank(variableDetails.get(x))) {
            measurements.get(x).add(variableDetails.get(x));
          }
        }
      }
    }

    return measurements;

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
