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
package esiptestbed.mudrod.ontology.process;

import java.util.Iterator;

import esiptestbed.mudrod.ontology.Ontology;

/**
 * @author lewismc
 *
 */
public class EsipCOROntology implements Ontology {

  /**
   * 
   */
  public EsipCOROntology() {
    //default constructor
  }


  @Override
  public void load() {
    // to be completed
  }

  /* (non-Javadoc)
   * @see esiptestbed.mudrod.ontology.Ontology#load(java.lang.String[])
   */
  @Override
  public void load(String[] urls) {
    // to be completed
  }

  /* (non-Javadoc)
   * @see esiptestbed.mudrod.ontology.Ontology#merge(esiptestbed.mudrod.ontology.Ontology)
   */
  @Override
  public void merge(Ontology o) {
    // to be completed
  }

  /* (non-Javadoc)
   * @see esiptestbed.mudrod.ontology.Ontology#subclasses(java.lang.String)
   */
  @Override
  public Iterator<String> subclasses(String entitySearchTerm) {
    return null;
  }

  /* (non-Javadoc)
   * @see esiptestbed.mudrod.ontology.Ontology#synonyms(java.lang.String)
   */
  @Override
  public Iterator<String> synonyms(String queryKeyPhrase) {
    return null;
  }

}
