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

import org.apache.jena.ontology.Individual;
import org.apache.jena.ontology.OntClass;
import org.apache.jena.ontology.OntModel;
import org.apache.jena.ontology.OntModelSpec;
import org.apache.jena.ontology.OntResource;
import org.apache.jena.ontology.Restriction;
import org.apache.jena.rdf.model.AnonId;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.shared.PrefixMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * The LocalOntology implementation enables us to work with Ontology files
 * whcih are cached locally and available on the runtime classpath e.g.
 * in <code>src/main/resource/ontology/...</code>.
 * From here we can test and iterate on how use of ontology can enhance search.
 */
public class LocalOntology implements Ontology {

  public static final Logger LOG = LoggerFactory.getLogger(LocalOntology.class);

  public static final String DELIMITER_SEARCHTERM = " ";

  private Map<Object, Object> searchTerms = new HashMap<>();
  private static OntologyParser parser;
  private static OntModel ontologyModel;
  private Ontology ontology;
  private static Map<AnonId, String> mAnonIDs = new HashMap<>();
  private static int mAnonCount = 0;
  private List<String> ontArrayList;

  public LocalOntology() {
    //only initialize all the static variables
    //if first time called to this ontology constructor
    if (ontology == null) {
      if (LOG.isInfoEnabled()) {
        LOG.info("Creating new ontology");
      }
      parser = new OwlParser();
      ontology = this;
    }
    if (ontologyModel == null)
      ontologyModel = ModelFactory.createOntologyModel(OntModelSpec.OWL_MEM, null);
    load();
  }

  /**
   * Static accessor for {@link LocalOntology}
   * instance implementation defined within <code>config.xml</code>.
   *
   * @return a {@link LocalOntology}
   */
  public Ontology getInstance() {
    if (ontology == null) {
      ontology = new LocalOntology();
    }
    return ontology;
  }

  /**
   * Load the default <i>sweetAll.owl</i> ontology
   * from <a href="https://raw.githubusercontent.com/ESIPFed/sweet/master/2.4/sweetAll.owl">
   * https://raw.githubusercontent.com/ESIPFed/sweet/master/2.4/sweetAll.owl</a>
   */
  @Override
  public void load() {
    URL ontURL = null;
    try {
      ontURL = new URL("https://raw.githubusercontent.com/ESIPFed/sweet/master/2.4/sweetAll.owl");
      //ontURL = new URL("https://raw.githubusercontent.com/ESIPFed/sweet/master/2.4/reprDataProduct.owl");
    } catch (MalformedURLException e) {
      LOG.error("Error when attempting to create URL resource: ", e);
    }
    ontArrayList = new ArrayList<>();
    try {
      ontArrayList.add(ontURL.toURI().toString());
    } catch (URISyntaxException e) {
      LOG.error("Error in URL syntax, please check your Ontology resource: ", e);
    }
    if (!ontArrayList.isEmpty()) {
      load(ontArrayList.stream().toArray(String[]::new));
    }
  }

  /**
   * Load a string array of local URIs which refernece .owl files.
   */
  @Override
  public void load(String[] urls) {
    for (int i = 0; i < urls.length; i++) {
      String url = urls[i].trim();
      if (!"".equals(url))
        if (LOG.isInfoEnabled()) {
          LOG.info("Reading and processing {}", url);
        }
      load(ontologyModel, url);
    }
    parser.parse(ontology, ontologyModel);
  }

  private void load(Object m, String url) {
    try {
      ((OntModel) m).read(url, null, null);
      LOG.info("Successfully processed {}", url);
    } catch (Exception e) {
      LOG.error("Failed whilst attempting to read ontology {}: Error: ", url, e);
    }
  }

  /**
   * Get the {@link gov.nasa.jpl.mudrod.ontology.process.OntologyParser}
   * implementation being used to process the input ontology resources.
   * @return an {@link gov.nasa.jpl.mudrod.ontology.process.OntologyParser} implementation
   */
  public OntologyParser getParser() {
    if (parser == null) {
      parser = new OwlParser();
    }
    return parser;
  }

  /**
   * Return the {@link org.apache.jena.ontology.OntModel} instance
   * which created from input ontology resources.
   * @return a constructed {@link org.apache.jena.ontology.OntModel}
   */
  public static OntModel getModel() {
    return ontologyModel;
  }

  /**
   * Return the loaded Ontology resources.
   * @return a {@link java.util.List} of resources.
   */
  public List<String> getLoadedOntologyResources() {
    if (ontArrayList != null) {
      return ontArrayList;
    } else {
      return new ArrayList<>();
    }
  }
  /**
   * Not yet implemented.
   */
  @Override
  public void merge(Ontology o) {
    // not yet implemented
  }

  /**
   * Retrieve all subclasses of entity(ies) hashed to searchTerm
   * @param entitySearchTerm a query (keywords) for which to obtain
   * subclasses.
   * @return an {@link java.util.Iterator} containing the subclass as Strings.
   */
  @Override
  public Iterator<String> subclasses(String entitySearchTerm) {
    Map<OntResource, String> classMap = retrieve(entitySearchTerm);
    Map<String, String> subclasses = new HashMap<>();

    Iterator<OntResource> iter = classMap.keySet().iterator();
    while (iter.hasNext()) {
      OntResource resource = iter.next();

      if (resource instanceof OntClass) {
        //get subclasses N.B. we only get direct sub-classes e.g. direct children
        //it is possible for us to navigate the entire class tree if we wish, we simply
        //need to pass the .listSubClasses(true) boolean parameter.
        for (Iterator<?> i = ((OntClass) resource).listSubClasses(); i.hasNext();) {
          OntResource subclass = (OntResource) i.next();
          for (Iterator<?> j = subclass.listLabels(null); j.hasNext();) {
            Literal l = (Literal) j.next();
            subclasses.put(l.toString(), "1");
          }
        }
        //get individuals
        for (Iterator<?> i = ((OntClass) resource).listInstances(); i.hasNext(); ) {
          OntResource subclass = (OntResource) i.next();
          for (Iterator<?> j = subclass.listLabels(null); j.hasNext();) {
            Literal l = (Literal) j.next();
            subclasses.put(l.toString(), "1");
          }
        }
      } else if (resource instanceof Individual) {
        for (Iterator<?> i = resource.listSameAs(); i.hasNext();) {
          OntResource subclass = (OntResource) i.next();
          for (Iterator<?> j = subclass.listLabels(null); j.hasNext();) {
            Literal l = (Literal) j.next();
            subclasses.put(l.toString(), "1");
          }
        }
      }
    }
    return subclasses.keySet().iterator();
  }

  /**
   * Retreives synonyms for an given phrase if the phrase
   * is present in the ontology
   * @param queryKeyPhrase an input string representing a phrase
   * for which we wish to obtain synonyms.
   * @return an {@link java.util.Iterator} containing synonyms string tokens
   * or an empty if no synonyms exist for the given queryKeyPhrase.
   */
  @Override
  public Iterator synonyms(String queryKeyPhrase) {

    Map<?, ?> classMap = retrieve(queryKeyPhrase);

    Map<Object, Object> synonyms = new HashMap<>();

    Iterator<?> iter = classMap.keySet().iterator();
    while (iter.hasNext()) {
      OntResource resource = (OntResource) iter.next();

      //listLabels
      for (Iterator<?> i = resource.listLabels(null); i.hasNext();) {
        Literal l = (Literal) i.next();
        synonyms.put(l.toString(), "1");
      }

      if (resource instanceof Individual) {
        //get all individuals same as this one
        for (Iterator<?> i = resource.listSameAs(); i.hasNext();) {
          Individual individual = (Individual) i.next();
          //add labels
          for (Iterator<?> j = individual.listLabels(null); j.hasNext();) {
            Literal l = (Literal) i.next();
            synonyms.put(l.toString(), "1");
          }
        }
      } else if (resource instanceof OntClass) {
        //list equivalent classes
        for (Iterator<?> i = ((OntClass) resource).listEquivalentClasses(); i.hasNext();) {
          OntClass equivClass = (OntClass) i.next();
          //add labels
          for (Iterator<?> j = equivClass.listLabels(null); j.hasNext();) {
            Literal l = (Literal) j.next();
            synonyms.put(l.toString(), "1");
          }
        }
      }
    }

    return synonyms.keySet().iterator();
  }

  public void addSearchTerm(String label, OntResource resource) {
    Map<OntResource, String> m = retrieve(label);
    if (m == null) {
      m = new HashMap<>();
    }
    m.put(resource, "1");
    searchTerms.put(label.toLowerCase(), m);
  }

  /**
   * A basic lookup function for retrieving keys (phrases or tokens)
   * from the ontology search terms map. Right now only exact lookups
   * will retrieve a result... this could be improved by using some
   * advanced parsing logic... such as Lucene query parser.
   * @param label the label (phrases or tokens) to retrieve from the 
   * ontology search terms map.
   * @return an {@link java.util.Map} if there are match(es)
   * or an empty {@link java.util.HashMap} if there are no
   * matches.
   */
  public Map<OntResource, String> retrieve(String label) {
    @SuppressWarnings("unchecked")
    Map<OntResource, String> m = (Map<OntResource, String>) searchTerms.get(label.toLowerCase());
    if (m == null) {
      m = new HashMap<>();
    }
    return m;
  }

  protected static void renderHierarchy(PrintStream out, OntClass cls, List<Object> occurs, int depth) {
    renderClassDescription(out, cls, depth);
    out.println();

    // recurse to the next level down
    if (cls.canAs(OntClass.class) && !occurs.contains(cls)) {
      for (Iterator<?> i = cls.listSubClasses(true); i.hasNext(); ) {
        OntClass sub = (OntClass) i.next();

        // we push this expression on the occurs list before we recurse
        occurs.add(cls);
        renderHierarchy(out, sub, occurs, depth + 1);
        occurs.remove(cls);
      }
      for (Iterator<?> i = cls.listInstances(); i.hasNext(); ) {
        Individual individual = (Individual) i.next();
        renderURI(out, individual.getModel(), individual.getURI());
        out.print(" [");
        for (Iterator<?> j = individual.listLabels(null); j.hasNext(); ) {
          out.print(((Literal) j.next()).getString() + ", ");
        }
        out.print("] ");
        out.println();
      }
    }
  }

  public static void renderClassDescription(PrintStream out, OntClass c, int depth) {
    indent(out, depth);

    if (c.isRestriction()) {
      renderRestriction(out, (Restriction) c.as(Restriction.class));
    } else {
      if (!c.isAnon()) {
        out.print("Class ");
        renderURI(out, c.getModel(), c.getURI());

        out.print(c.getLocalName());

        out.print(" [");
        for (Iterator<?> i = c.listLabels(null); i.hasNext(); ) {
          out.print(((Literal) i.next()).getString() + ", ");
        }
        out.print("] ");
      } else {
        renderAnonymous(out, c, "class");
      }
    }
  }

  protected static void renderRestriction(PrintStream out, Restriction r) {
    if (!r.isAnon()) {
      out.print("Restriction ");
      renderURI(out, r.getModel(), r.getURI());
    } else {
      renderAnonymous(out, r, "restriction");
    }

    out.print(" on property ");
    renderURI(out, r.getModel(), r.getOnProperty().getURI());
  }

  protected static void renderURI(PrintStream out, PrefixMapping prefixes, String uri) {
    out.print(prefixes.expandPrefix(uri));
  }

  protected static void renderAnonymous(PrintStream out, Resource anon, String name) {
    String anonID = mAnonIDs.get(anon.getId());
    if (anonID == null) {
      anonID = "a-" + mAnonCount++;
      mAnonIDs.put(anon.getId(), anonID);
    }

    out.print("Anonymous ");
    out.print(name);
    out.print(" with ID ");
    out.print(anonID);
  }

  protected static void indent(PrintStream out, int depth) {
    for (int i = 0; i < depth; i++) {
      out.print(" ");
    }
  }

}
