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

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import esiptestbed.mudrod.main.MudrodConstants;
import esiptestbed.mudrod.ontology.process.EsipSRIOntology;
import esiptestbed.mudrod.ontology.process.LocalOntology;

/**
 * The mechanism for creating an {@link esiptestbed.mudrod.ontology.Ontology}
 * implementation. The {@link esiptestbed.mudrod.ontology.Ontology} implementation 
 * should be specified in 
 * <a href="https://github.com/mudrod/mudrod/blob/master/core/src/main/resources/config.xml">
 * config.xml</a> with configuration key
 * <code>mudrod.ontology.implementation</code>.
 * This property can also be accessed via
 * {@link esiptestbed.mudrod.main.MudrodConstants#ONTOLOGY_IMPL}.
 * @author lewismc
 */
public class OntologyFactory {

  public static final Logger LOG = LoggerFactory.getLogger(OntologyFactory.class);

  private Properties props;

  /**
   * The mechanism for creating an {@link esiptestbed.mudrod.ontology.Ontology}
   * implementation.
   * @param props a populated Mudrod {@link java.util.Properties} object.
   */
  public OntologyFactory(Properties props) {
    this.props = props;
  }

  /**
   * Obtain the {@link esiptestbed.mudrod.ontology.Ontology}
   * implementation for use within Mudrod.
   * @return Returns the ontology implementation specified
   * in <a href="https://github.com/mudrod/mudrod/blob/master/core/src/main/resources/config.xml">
   * config.xml</a> with configuration key
   * <code>mudrod.ontology.implementation</code>. This property can also be accessed via
   * {@link esiptestbed.mudrod.main.MudrodConstants#ONTOLOGY_IMPL}.
   */
  public Ontology getOntology() {

    String ontologyImpl = this.props.getProperty(MudrodConstants.ONTOLOGY_IMPL, "Local");

    if (LOG.isInfoEnabled()) {
      LOG.info("Using ontology extension: " + ontologyImpl);
    }
    Ontology ontImpl = null;
    switch (ontologyImpl) {
    case "EsipSRI":
      ontImpl = new EsipSRIOntology();
      break;
    case "Local":
      ontImpl = new LocalOntology();
      break;
    default:
      LOG.error("The ontology implementation defined within config.xml key 'mudrod.ontology.implementation'"
          + " is not recognized. Options include 'EsipSRI' and 'Local'. Please fix and recompile.");
    }
    ontImpl.load();
    return ontImpl;
  }

}
