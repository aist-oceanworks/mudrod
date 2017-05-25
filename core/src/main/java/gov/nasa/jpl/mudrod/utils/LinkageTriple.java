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
package gov.nasa.jpl.mudrod.utils;

import gov.nasa.jpl.mudrod.driver.ESDriver;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * ClassName: LinkageTriple Function: Vocabulary linkage operations
 */
public class LinkageTriple implements Serializable {

  /**
   *
   */
  private static final long serialVersionUID = 1L;
  // keyAId: ID of term A
  public long keyAId;
  // keyBId: ID of term B
  public long keyBId;
  // weight: The relationship between term A and Term B
  public double weight;
  // keyA: TermA
  public String keyA;
  // keyB: TermB
  public String keyB;
  // df: Format number
  public static DecimalFormat df = new DecimalFormat("#.00");

  public LinkageTriple() {
    // TODO Auto-generated constructor stub
  }

  /**
   * TODO Output linkage triples in string format.
   *
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return keyA + "," + keyB + ":" + weight;
  }

  public static void insertTriples(ESDriver es, List<LinkageTriple> triples, String index, String type) throws IOException {
    LinkageTriple.insertTriples(es, triples, index, type, false, false);
  }

  public static void insertTriples(ESDriver es, List<LinkageTriple> triples, String index, String type, Boolean bTriple, boolean bSymmetry) throws IOException {
    es.deleteType(index, type);
    if (bTriple) {
      LinkageTriple.addMapping(es, index, type);
    }

    if (triples == null) {
      return;
    }

    es.createBulkProcessor();
    int size = triples.size();
    for (int i = 0; i < size; i++) {

      XContentBuilder jsonBuilder = jsonBuilder().startObject();
      if (bTriple) {

        jsonBuilder.field("concept_A", triples.get(i).keyA);
        jsonBuilder.field("concept_B", triples.get(i).keyB);

      } else {
        jsonBuilder.field("keywords", triples.get(i).keyA + "," + triples.get(i).keyB);
      }

      jsonBuilder.field("weight", Double.parseDouble(df.format(triples.get(i).weight)));
      jsonBuilder.endObject();

      IndexRequest ir = new IndexRequest(index, type).source(jsonBuilder);
      es.getBulkProcessor().add(ir);

      if (bTriple && bSymmetry) {
        XContentBuilder symmetryJsonBuilder = jsonBuilder().startObject();
        symmetryJsonBuilder.field("concept_A", triples.get(i).keyB);
        symmetryJsonBuilder.field("concept_B", triples.get(i).keyA);

        symmetryJsonBuilder.field("weight", Double.parseDouble(df.format(triples.get(i).weight)));

        symmetryJsonBuilder.endObject();

        IndexRequest symmetryir = new IndexRequest(index, type).source(symmetryJsonBuilder);
        es.getBulkProcessor().add(symmetryir);
      }
    }
    es.destroyBulkProcessor();
  }

  public static void addMapping(ESDriver es, String index, String type) {
    XContentBuilder Mapping;
    try {
      Mapping = jsonBuilder().startObject().startObject(type).startObject("properties").startObject("concept_A").field("type", "string").field("index", "not_analyzed").endObject()
          .startObject("concept_B").field("type", "string").field("index", "not_analyzed").endObject()

          .endObject().endObject().endObject();

      es.getClient().admin().indices().preparePutMapping(index).setType(type).setSource(Mapping).execute().actionGet();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void standardTriples(ESDriver es, String index, String type) throws IOException {
    es.createBulkProcessor();

    SearchResponse sr = es.getClient().prepareSearch(index).setTypes(type).setQuery(QueryBuilders.matchAllQuery()).setSize(0)
        .addAggregation(AggregationBuilders.terms("concepts").field("concept_A").size(0)).execute().actionGet();
    Terms concepts = sr.getAggregations().get("concepts");

    for (Terms.Bucket entry : concepts.getBuckets()) {
      String concept = (String) entry.getKey();
      double maxSim = LinkageTriple.getMaxSimilarity(es, index, type, concept);
      if (maxSim == 1.0) {
        continue;
      }

      SearchResponse scrollResp = es.getClient().prepareSearch(index).setTypes(type).setScroll(new TimeValue(60000)).setQuery(QueryBuilders.termQuery("concept_A", concept))
          .addSort("weight", SortOrder.DESC).setSize(100).execute().actionGet();

      while (true) {
        for (SearchHit hit : scrollResp.getHits().getHits()) {
          Map<String, Object> metadata = hit.getSource();
          double sim = (double) metadata.get("weight");
          double newSim = sim / maxSim;
          UpdateRequest ur = es.generateUpdateRequest(index, type, hit.getId(), "weight", Double.parseDouble(df.format(newSim)));
          es.getBulkProcessor().add(ur);
        }

        scrollResp = es.getClient().prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
        if (scrollResp.getHits().getHits().length == 0) {
          break;
        }
      }
    }

    es.destroyBulkProcessor();
  }

  private static double getMaxSimilarity(ESDriver es, String index, String type, String concept) {

    double maxSim = 1.0;
    SearchRequestBuilder builder = es.getClient().prepareSearch(index).setTypes(type).setQuery(QueryBuilders.termQuery("concept_A", concept)).addSort("weight", SortOrder.DESC).setSize(1);

    SearchResponse usrhis = builder.execute().actionGet();
    SearchHit[] hits = usrhis.getHits().getHits();
    if (hits.length == 1) {
      SearchHit hit = hits[0];
      Map<String, Object> result = hit.getSource();
      maxSim = (double) result.get("weight");
    }

    if (maxSim == 0.0) {
      maxSim = 1.0;
    }

    return maxSim;
  }
}
