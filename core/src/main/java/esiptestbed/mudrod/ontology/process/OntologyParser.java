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

import org.apache.jena.ontology.OntClass;
import org.apache.jena.ontology.OntModel;

/**
 * Interface for specific ontology parsers e.g. .ttl, RDFXML,
 * etc.
 * @author lewismc
 *
 */
public interface OntologyParser {
  
  /**
   * An ontology model (RDF graph) to parse for literals.
   * @param ontModel the {@link org.apache.jena.ontology.OntModel}
   */
  public void parse(OntModel ontModel);
  
  /**
   * An ontology model (RDF graph) for which to obtain an
   * {@link java.util.Iterator} instance of all root classes.
   * @param ontModel the {@link org.apache.jena.ontology.OntModel}
   * @return an {@link java.util.Iterator} instance containing all root classes.
   */
  public Iterator<OntClass> rootClasses(OntModel ontModel);

}
