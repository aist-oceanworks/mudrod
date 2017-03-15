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
package gov.nasa.jpl.mudrod.ontology.process;

import gov.nasa.jpl.mudrod.ontology.Ontology;

import java.util.Iterator;

/**
 * @author lewismc
 */
public class EsipPortalOntology implements Ontology {

  /**
   *
   */
  public EsipPortalOntology() {
    //default constructor
  }

  /* (non-Javadoc)
   * @see Ontology#load(java.lang.String[])
   */
  @Override
  public void load(String[] urls) {
    // to be completed
  }

  /* (non-Javadoc)
   * @see Ontology#load()
   */
  @Override
  public void load() {
    // to be completed
  }

  /* (non-Javadoc)
   * @see Ontology#merge(Ontology)
   */
  @Override
  public void merge(Ontology o) {
    // to be completed
  }

  /* (non-Javadoc)
   * @see Ontology#subclasses(java.lang.String)
   */
  @Override
  public Iterator<String> subclasses(String entitySearchTerm) {
    return null;
  }

  /* (non-Javadoc)
   * @see Ontology#synonyms(java.lang.String)
   */
  @Override
  public Iterator<String> synonyms(String queryKeyPhrase) {
    return null;
  }

}
