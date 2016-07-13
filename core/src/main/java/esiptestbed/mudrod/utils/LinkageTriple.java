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
package esiptestbed.mudrod.utils;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.List;

import org.elasticsearch.action.index.IndexRequest;

import esiptestbed.mudrod.driver.ESDriver;

public class LinkageTriple implements Serializable {

  public long keyAId;
  public long keyBId;
  public double weight;
  public String keyA;
  public String keyB;
  public static DecimalFormat df = new DecimalFormat("#.00");

  public LinkageTriple() {
    // TODO Auto-generated constructor stub
  }

  public String toString() {
    return keyA + "," + keyB + ":" + weight;
  }

  public static void insertTriples(ESDriver es, List<LinkageTriple> triples,
      String index, String type) throws IOException {
    es.deleteType(index, type);

    es.createBulkProcesser();
    int size = triples.size();
    for (int i = 0; i < size; i++) {
      IndexRequest ir = new IndexRequest(index, type).source(jsonBuilder()
          .startObject()
          .field("keywords", triples.get(i).keyA + "," + triples.get(i).keyB)
          .field("weight", Double.parseDouble(df.format(triples.get(i).weight)))
          .endObject());
      es.bulkProcessor.add(ir);
    }
    es.destroyBulkProcessor();
  }
}
