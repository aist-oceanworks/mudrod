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
package gov.nasa.jpl.mudrod.ontology;

import gov.nasa.jpl.mudrod.main.MudrodConstants;
import gov.nasa.jpl.mudrod.ontology.process.EsipCOROntology;
import gov.nasa.jpl.mudrod.ontology.process.EsipPortalOntology;
import gov.nasa.jpl.mudrod.ontology.process.LocalOntology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * The mechanism for creating an {@link Ontology}
 * implementation. The {@link Ontology} implementation
 * should be specified in
 * <a href="https://github.com/mudrod/mudrod/blob/master/core/src/main/resources/config.xml">
 * config.xml</a> with configuration key
 * <code>mudrod.ontology.implementation</code>.
 * This property can also be accessed via
 * {@link MudrodConstants#ONTOLOGY_IMPL}.
 *
 * @author lewismc
 */
public class OntologyFactory {

  public static final Logger LOG = LoggerFactory.getLogger(OntologyFactory.class);

  private Properties props;

  /**
   * The mechanism for creating an {@link Ontology}
   * implementation.
   *
   * @param props a populated Mudrod {@link java.util.Properties} object.
   */
  public OntologyFactory(Properties props) {
    this.props = props;
  }

  /**
   * Obtain the {@link Ontology}
   * implementation for use within Mudrod.
   *
   * @return Returns the ontology implementation specified
   * in <a href="https://github.com/mudrod/mudrod/blob/master/core/src/main/resources/config.xml">
   * config.xml</a> with configuration key
   * <code>mudrod.ontology.implementation</code>. This property can also be accessed via
   * {@link MudrodConstants#ONTOLOGY_IMPL}.
   */
  public Ontology getOntology() {

    String ontologyImpl = this.props.getProperty(MudrodConstants.ONTOLOGY_IMPL, "Local");

    LOG.info("Using ontology extension: {}", ontologyImpl);
    Ontology ontImpl;
    switch (ontologyImpl) {
    case "EsipCOR":
      ontImpl = new EsipCOROntology();
      break;
    case "EsipPortal":
      ontImpl = new EsipPortalOntology();
      break;
    default:
      ontImpl = new LocalOntology();
      break;
    }
    return ontImpl;
  }

}
