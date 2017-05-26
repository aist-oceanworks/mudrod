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

import org.apache.jena.ontology.Individual;
import org.apache.jena.ontology.OntClass;
import org.apache.jena.ontology.OntModel;
import org.apache.jena.rdf.model.Literal;

import com.esotericsoftware.minlog.Log;

import gov.nasa.jpl.mudrod.ontology.Ontology;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@link gov.nasa.jpl.mudrod.ontology.process.OntologyParser}
 * implementation for <a href="http://www.w3.org/TR/owl-features/">W3C OWL</a> 
 * files.
 */
public class OwlParser implements OntologyParser {
  
  private Ontology ont;

  public OwlParser() {
    //default constructor
  }

  /**
   * Parse OWL ontology files using Apache Jena
   */
  @Override
  public void parse(Ontology ont, OntModel m) {
    this.ont = ont;
    for (Iterator<OntClass> i = rootClasses(m); i.hasNext(); ) {
      OntClass c = i.next();

      //dont deal with anonymous classes
      if (c.isAnon()) {
        continue;
      }

      parseClass(c, new ArrayList<>(), 0);
    }
  }

  protected void parseClass(OntClass cls, List<Object> occurs, int depth) {
    //dont deal with anonymous classes
    if (cls.isAnon()) {
      return;
    }

    //add cls to Ontology searchterms
    //list labels
    Iterator<?> labelIter = cls.listLabels(null);
    //if has no labels
    if (!labelIter.hasNext()) {
      //add rdf:ID as a label
      cls.addLabel(rdfidToLabel(cls.getLocalName()), null);
    }
    //reset the label iterator
    labelIter = cls.listLabels(null);

    while (labelIter.hasNext()) {
      Literal l = (Literal) labelIter.next();
      ((LocalOntology) ont).addSearchTerm(l.toString(), cls);
    }

    // recurse to the next level down
    if (cls.canAs(OntClass.class) && !occurs.contains(cls)) {
      //list subclasses
      for (Iterator<?> i = cls.listSubClasses(true); i.hasNext(); ) {
        OntClass sub = (OntClass) i.next();

        // we push this expression on the occurs list before we recurse
        occurs.add(cls);
        parseClass(sub, occurs, depth + 1);
        occurs.remove(cls);
      }

      //list instances
      for (Iterator<?> i = cls.listInstances(); i.hasNext(); ) {
        //add search terms for each instance

        //list labels
        Individual individual = (Individual) i.next();
        for (Iterator<?> j = individual.listLabels(null); j.hasNext(); ) {
          Literal l = (Literal) j.next();
          ((LocalOntology) ont).addSearchTerm(l.toString(), individual);
        }
      }
    }
  }

  /**
   * Parses out all root classes of the given 
   * {@link org.apache.jena.ontology.OntModel}
   * TODO This implementation DOES NOT currently handle
   * aggregate/collection ontologies e.g. ontologies which have ONLY imports,
   * and NO root classes such as 
   * <a href="https://raw.githubusercontent.com/ESIPFed/sweet/master/2.4/sweetAll.owl">sweetAll.owl</a>.
   * @param m the {@link org.apache.jena.ontology.OntModel} we wish to obtain 
   * all root classes for.
   * @return an {@link java.util.Iterator} of {@link org.apache.jena.ontology.OntClass}
   * elements representing all root classes.
   */
  @Override
  public Iterator<OntClass> rootClasses(OntModel m) {
    List<OntClass> roots = new ArrayList<>();

    for (Iterator<?> i = m.listClasses(); i.hasNext(); ) {
      OntClass c = (OntClass) i.next();
      try {
        // too confusing to list all the restrictions as root classes 
        if (c.isAnon()) {
          continue;
        }

        if (c.hasSuperClass(m.getProfile().THING(), true)) {
          // this class is directly descended from Thing
          roots.add(c);
        } else if (c.getCardinality(m.getProfile().SUB_CLASS_OF()) == 0) {
          // this class has no super-classes (can occur if we're not using the reasoner)
          roots.add(c);
        }
      } catch (Exception e) {
        Log.error("Error during extraction or root Classes from Ontology Model: ", e);
      }
    }

    return roots.iterator();
  }

  public String rdfidToLabel(String idString) {
    Pattern p = Pattern.compile("([a-z0-9])([A-Z])");
    Matcher m = p.matcher(idString);

    String labelString = idString;
    while (m.find()) {
      labelString = labelString.replaceAll(m.group(1) + m.group(2), m.group(1) + " " + m.group(2));
    }
    return labelString;
  }

}
