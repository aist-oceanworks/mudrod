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
package esiptestbed.mudrod.recommendation.structure;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.sree.textbytes.jtopia.Configuration;
import com.sree.textbytes.jtopia.TermDocument;
import com.sree.textbytes.jtopia.TermsExtractor;

import esiptestbed.mudrod.driver.ESDriver;

/**
 * Encode metadata using one hot method
 */
public class JTopiaTermExtractor {

  private String indexName;
  private String metadataType;
  private String jtopiaDir;

  /**
   * Creates a new instance of OHEncoder.
   *
   * @param props
   *          the Mudrod configuration
   */
  public JTopiaTermExtractor(Properties props) {
    indexName = props.getProperty("indexName");
    metadataType = props.getProperty("recom_metadataType");
    jtopiaDir = props.getProperty("jtopia");
  }

  public void extractTermIndependently(ESDriver es) throws IOException {

    es.createBulkProcessor();

    // for test
    File file = new File("d:/test.txt");
    if (file.exists()) {
      file.delete();
    }

    file.createNewFile();
    FileWriter fw = new FileWriter(file.getAbsoluteFile());
    BufferedWriter bw = new BufferedWriter(fw);

    SearchResponse scrollResp = es.getClient().prepareSearch(indexName)
        .setTypes(metadataType).setScroll(new TimeValue(60000))
        .setQuery(QueryBuilders.matchAllQuery()).setSize(100).execute()
        .actionGet();

    while (true) {
      for (SearchHit hit : scrollResp.getHits().getHits()) {
        Map<String, Object> metadata = hit.getSource();

        String shortname = (String) metadata.get("Dataset-ShortName");
        /*if (!shortname.equals("AQUARIUS_L3_SSS_SMIA_SEASONAL-CLIMATOLOGY_V4")) {
          continue;
        }*/

        String longname = (String) metadata.get("Dataset-LongName");
        String description = (String) metadata.get("Dataset-Description");
        Object variable = metadata.get("DatasetParameter-Variable");
        Object term = metadata.get("DatasetParameter-Term");

        String filedStr = longname + "." + description;

        Map<String, ArrayList<Integer>> terms = this.extractTerm(filedStr);
        String extractd = "";
        for (String extractTerm : terms.keySet()) {
          int occurance = terms.get(extractTerm).get(0);
          for (int i = 0; i < occurance; i++) {
            extractd += extractTerm + ",";
          }
        }

        if (term != null) {
          String termStr = term.toString().replaceAll("\\[", "")
              .replaceAll("\\]", "");
          termStr = termStr.replaceAll("\\/", ",");
          extractd += termStr + ", ";
        }

        if (variable != null) {
          String variableStr = term.toString().replaceAll("\\[", "")
              .replaceAll("\\]", "");
          variableStr = variableStr.replaceAll("\\/", ",");

          extractd += variableStr + ", ";
        }

        extractd += shortname.replaceAll("_", ",");

        UpdateRequest ur = es.generateUpdateRequest(indexName, metadataType,
            hit.getId(), "Dataset-ExtractTerm", extractd);
        es.getBulkProcessor().add(ur);

        bw.write(extractd + "\n");
      }

      scrollResp = es.getClient().prepareSearchScroll(scrollResp.getScrollId())
          .setScroll(new TimeValue(600000)).execute().actionGet();
      if (scrollResp.getHits().getHits().length == 0) {
        break;
      }
    }

    bw.close();
    es.destroyBulkProcessor();
  }

  private Map<String, ArrayList<Integer>> extractTerm(String input) {
    Configuration.setTaggerType("default");// "default" for lexicon POS
    String modelfile = jtopiaDir + "/default/english-lexicon.txt";
    System.out.println(modelfile);
    // tagger
    // Configuration.setTaggerType("standford");// and "openNLP" for openNLP POS
    // String modelfile = jtopiaDir
    // + "/stanford/english-left3words-distsim.tagger";
    // Configuration.setTaggerType("openNLP");
    // String modelfile = jtopiaDir + "/openNLP/en-pos-maxent.bin";
    // tagger
    Configuration.setSingleStrength(2);
    Configuration.setNoLimitStrength(2);
    Configuration.setModelFileLocation(modelfile);

    TermsExtractor termExtractor = new TermsExtractor();
    TermDocument termDocument = new TermDocument();
    termDocument = termExtractor.extractTerms(input);
    Map<String, ArrayList<Integer>> termMaps = termDocument
        .getFinalFilteredTerms();
    return termMaps;
  }

  public static void main(String[] args) throws IOException {

  }
}
