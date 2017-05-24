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

import gov.nasa.jpl.mudrod.main.MudrodEngine;
import gov.nasa.jpl.mudrod.ontology.Ontology;
import gov.nasa.jpl.mudrod.ontology.OntologyFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
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

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * The LocalOntology implementation enables us to work with Ontology files
 * whcih are cached locally and available on the runtime classpath e.g.
 * in <code>src/main/resource/ontology/...</code>.
 * From here we can test and iterate on how use of ontology can enhance search.
 */
public class LocalOntology implements Ontology {

  public static final Logger LOG = LoggerFactory.getLogger(LocalOntology.class);

  public static final String DELIMITER_SEARCHTERM = " ";

  private static String ONT_DIR = "ontDir";
  private static Map<Object, Object> searchTerms = new HashMap<>();
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
    parser.parse(ontologyModel);
  }

  private void load(Object m, String url) {
    try {
      ((OntModel) m).read(url, null, null);
    } catch (Exception e) {
      LOG.error("Failed whilst attempting to read ontology {}: Error: ", url, e);
    }
  }

  /**
   * Get the {@link gov.nasa.jpl.mudrod.ontology.process.OntologyParser}
   * implementation being used to process the input ontology resources.
   * @return an {@link gov.nasa.jpl.mudrod.ontology.process.OntologyParser} implementation
   */
  public static OntologyParser getParser() {
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
        //get subclasses
        for (Iterator<?> i = ((OntClass) resource).listSubClasses(); i.hasNext(); ) {
          OntResource subclass = (OntResource) i.next();
          for (Iterator<?> j = subclass.listLabels(null); j.hasNext(); ) {
            Literal l = (Literal) j.next();
            subclasses.put(l.toString(), "1");
          }
        }
        //get individuals
        for (Iterator<?> i = ((OntClass) resource).listInstances(); i.hasNext(); ) {
          OntResource subclass = (OntResource) i.next();
          for (Iterator<?> j = subclass.listLabels(null); j.hasNext(); ) {
            Literal l = (Literal) j.next();
            subclasses.put(l.toString(), "1");
          }
        }
      } else if (resource instanceof Individual) {
        for (Iterator<?> i = resource.listSameAs(); i.hasNext(); ) {
          OntResource subclass = (OntResource) i.next();
          for (Iterator<?> j = subclass.listLabels(null); j.hasNext(); ) {
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
    //need to have a html quote method instead
    String qKeyPhrase = queryKeyPhrase.replaceAll("\\s+", "\\+");

    Map<?, ?> classMap = retrieve(qKeyPhrase);

    Map<Object, Object> synonyms = new HashMap<>();

    Iterator<?> iter = classMap.keySet().iterator();
    while (iter.hasNext()) {
      OntResource resource = (OntResource) iter.next();

      //listLabels
      for (Iterator<?> i = resource.listLabels(null); i.hasNext(); ) {
        Literal l = (Literal) i.next();
        synonyms.put(l.toString(), "1");
      }

      if (resource instanceof Individual) {
        //get all individuals same as this one
        for (Iterator<?> i = resource.listSameAs(); i.hasNext(); ) {
          Individual individual = (Individual) i.next();
          //add labels
          for (Iterator<?> j = individual.listLabels(null); j.hasNext(); ) {
            Literal l = (Literal) i.next();
            synonyms.put(l.toString(), "1");
          }
        }
      } else if (resource instanceof OntClass) {
        //list equivalent classes
        for (Iterator<?> i = ((OntClass) resource).listEquivalentClasses(); i.hasNext(); ) {
          OntClass equivClass = (OntClass) i.next();
          //add labels
          for (Iterator<?> j = equivClass.listLabels(null); j.hasNext(); ) {
            Literal l = (Literal) j.next();
            synonyms.put(l.toString(), "1");
          }
        }
      }
    }

    return synonyms.keySet().iterator();
  }

  public static void addSearchTerm(String label, OntResource resource) {
    Map<OntResource, String> m = retrieve(label);
    if (m == null) {
      m = new HashMap<>();
    }
    m.put(resource, "1");
    searchTerms.put(label.toLowerCase(), m);
  }

  public static Map<OntResource, String> retrieve(String label) {
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

  public static void main(String[] args) throws Exception {

    // boolean options
    Option helpOpt = new Option("h", "help", false, "show this help message");
    // argument options
    Option ontDirOpt = OptionBuilder.hasArg(true).withArgName(ONT_DIR).withDescription("A directory containing .owl files.").isRequired(false).create();

    // create the options
    Options options = new Options();
    options.addOption(helpOpt);
    options.addOption(ontDirOpt);

    String ontDir;
    CommandLineParser parser = new GnuParser();
    try {
      CommandLine line = parser.parse(options, args);

      if (line.hasOption(ONT_DIR)) {
        ontDir = line.getOptionValue(ONT_DIR).replace("\\", "/");
      } else {
        ontDir = LocalOntology.class.getClassLoader().getResource("ontology").getFile();
      }
      if (!ontDir.endsWith("/")) {
        ontDir += "/";
      }
    } catch (Exception e) {
      LOG.error("Error whilst processing main method of LocalOntology.", e);
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("LocalOntology: 'ontDir' argument is mandatory. ", options, true);
      return;
    }
    File fileDir = new File(ontDir);
    //Fail if the input is not a directory.
    if (fileDir.isDirectory()) {
      List<String> owlFiles = new ArrayList<>();
      for (File owlFile : fileDir.listFiles()) {
        owlFiles.add(owlFile.toString());
      }
      MudrodEngine mEngine = new MudrodEngine();
      Properties props = mEngine.loadConfig();
      Ontology ontology = new OntologyFactory(props).getOntology();
      //convert to correct input for ontology loading.
      String[] owlArray = new String[owlFiles.size()];
      owlArray = owlFiles.toArray(owlArray);
      ontology.load(owlArray);

      String[] terms = new String[] { "Glacier ice" };
      //Demonstrate that we can do basic ontology heirarchy navigation and log output.
      for (Iterator<OntClass> i = getParser().rootClasses(getModel()); i.hasNext(); ) {

        //print Ontology Class Hierarchy
        OntClass c = i.next();
        renderHierarchy(System.out, c, new LinkedList<>(), 0);

        for (Iterator<OntClass> subClass = c.listSubClasses(true); subClass.hasNext(); ) {
          OntClass sub = subClass.next();
          //This means that the search term is present as an OntClass
          if (terms[0].equalsIgnoreCase(sub.getLabel(null))) {
            //Add the search term(s) above to the term cache.
            for (int j = 0; j < terms.length; j++) {
              addSearchTerm(terms[j], sub);
            }

            //Query the ontology and return subclasses of the search term(s)
            for (int k = 0; k < terms.length; k++) {
              Iterator<String> iter = ontology.subclasses(terms[k]);
              while (iter.hasNext()) {
                LOG.info("Subclasses >> " + iter.next());
              }
            }

            //print any synonymic relationships to demonstrate that we can 
            //undertake synonym-based query expansion
            for (int l = 0; l < terms.length; l++) {
              Iterator<String> iter = ontology.synonyms(terms[l]);
              while (iter.hasNext()) {
                LOG.info("Synonym >> " + iter.next());
              }
            }
          }
        }
      }

      mEngine.end();
    }

  }

}
