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
package esiptestbed.mudrod.ontology;

import java.util.Iterator;

/**
 * Base class for working with ontologies. Methods indicate ability
 * to load, merge e.g. merge relevant ontology subgraphs into a new
 * subgraph which can be used within Mudrod, subclass retreival,
 * synonym expansion, etc.
 * 
 * @author lewismc
 *
 */
public interface Ontology {

  /**
   * Indicates loading of URLs containing ontology resources
   * @param urls a {@link java.lang.String} containing ontology URIs.
   */
  public void load(String[] urls);

  /**
   * merge relevant ontology subgraphs into a new subgraph which can 
   * be used within Mudrod
   * @param o an ontology to merge with the current ontology held
   * within Mudrod.
   */
  public void merge(Ontology o);

  /**
   * Retreive all subclasses for a particular entity provided within the 
   * search term e.g.subclass-based query expansion.
   * @param entitySearchTerm an input search term
   * @return
   */
  public Iterator<String> subclasses(String entitySearchTerm);

  /**
   * Retreive all synonyms for a particular entity provided within the 
   * search term e.g.synonym-based query expansion.
   * @param queryKeyPhrase
   * @return
   */
  public Iterator<String> synonyms(String queryKeyPhrase);

}
