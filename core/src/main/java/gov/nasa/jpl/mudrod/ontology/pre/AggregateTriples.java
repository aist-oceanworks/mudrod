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
package gov.nasa.jpl.mudrod.ontology.pre;

import gov.nasa.jpl.mudrod.discoveryengine.DiscoveryStepAbstract;
import gov.nasa.jpl.mudrod.driver.ESDriver;
import gov.nasa.jpl.mudrod.driver.SparkDriver;
import org.apache.commons.io.FilenameUtils;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.Namespace;
import org.jdom2.filter.ElementFilter;
import org.jdom2.input.SAXBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * Supports ability to extract triples (subclassOf, equivalent class) from OWL file
 */
public class AggregateTriples extends DiscoveryStepAbstract {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(AggregateTriples.class);

  public AggregateTriples(Properties props, ESDriver es, SparkDriver spark) {
    super(props, es, spark);
  }

  /**
   * Method of executing triple aggregation
   */
  @Override
  public Object execute() {
    File file = new File(this.props.getProperty("oceanTriples"));
    if (file.exists()) {
      file.delete();
    }
    try {
      file.createNewFile();
    } catch (IOException e2) {
      e2.printStackTrace();
    }

    FileWriter fw;
    try {
      fw = new FileWriter(file.getAbsoluteFile());
      bw = new BufferedWriter(fw);
    } catch (IOException e) {
      e.printStackTrace();
    }

    File[] files = new File(this.props.getProperty("ontologyInputDir")).listFiles();
    for (File file_in : files) {
      String ext = FilenameUtils.getExtension(file_in.getAbsolutePath());
      if ("owl".equals(ext)) {
        try {
          loadxml(file_in.getAbsolutePath());
          getAllClass();
        } catch (JDOMException e1) {
          e1.printStackTrace();
        } catch (IOException e1) {
          e1.printStackTrace();
        }

      }
    }

    try {
      bw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  public Document document;
  public Element rootNode = null;
  final static String owl_namespace = "http://www.w3.org/2002/07/owl#";
  final static String rdf_namespace = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
  final static String rdfs_namespace = "http://www.w3.org/2000/01/rdf-schema#";

  BufferedWriter bw = null;

  /**
   * Load OWL file into memory
   *
   * @param filePathName local path of OWL file
   * @throws JDOMException JDOMException
   * @throws IOException   IOException
   */
  public void loadxml(String filePathName) throws JDOMException, IOException {
    SAXBuilder saxBuilder = new SAXBuilder();
    File file = new File(filePathName);

    document = saxBuilder.build(file);
    rootNode = document.getRootElement();
  }

  /**
   * Method of going through OWL structure
   */
  public void loopxml() {
    Iterator<?> processDescendants = rootNode.getDescendants(new ElementFilter());
    String text = "";

    while (processDescendants.hasNext()) {
      Element e = (Element) processDescendants.next();
      String currentName = e.getName();
      text = e.getTextTrim();
      if ("".equals(text)) {
        LOG.info(currentName);
      } else {
        LOG.info("{} : {}", currentName, text);
      }
    }
  }

  /**
   * Method of identifying a specific child given a element name
   *
   * @param str element name
   * @param ele parent element
   * @return the element of child
   */
  public Element findChild(String str, Element ele) {
    Iterator<?> processDescendants = ele.getDescendants(new ElementFilter());
    String name = "";
    Element result = null;

    while (processDescendants.hasNext()) {
      Element e = (Element) processDescendants.next();
      name = e.getName();
      if (name.equals(str)) {
        result = e;
        return result;
      }
    }
    return result;

  }

  /**
   * Method of extract triples (subclassOf, equivalent class) from OWL file
   *
   * @throws IOException IOException
   */
  public void getAllClass() throws IOException {
    List<?> classElements = rootNode.getChildren("Class", Namespace.getNamespace("owl", owl_namespace));

    for (int i = 0; i < classElements.size(); i++) {
      Element classElement = (Element) classElements.get(i);
      String className = classElement.getAttributeValue("about", Namespace.getNamespace("rdf", rdf_namespace));

      if (className == null) {
        className = classElement.getAttributeValue("ID", Namespace.getNamespace("rdf", rdf_namespace));
      }

      List<?> subclassElements = classElement.getChildren("subClassOf", Namespace.getNamespace("rdfs", rdfs_namespace));
      for (int j = 0; j < subclassElements.size(); j++) {
        Element subclassElement = (Element) subclassElements.get(j);
        String subclassName = subclassElement.getAttributeValue("resource", Namespace.getNamespace("rdf", rdf_namespace));
        if (subclassName == null) {
          Element allValuesFromEle = findChild("allValuesFrom", subclassElement);
          if (allValuesFromEle != null) {
            subclassName = allValuesFromEle.getAttributeValue("resource", Namespace.getNamespace("rdf", rdf_namespace));
            bw.write(cutString(className) + ",SubClassOf," + cutString(subclassName) + "\n");
          }
        } else {
          bw.write(cutString(className) + ",SubClassOf," + cutString(subclassName) + "\n");
        }

      }

      List equalClassElements = classElement.getChildren("equivalentClass", Namespace.getNamespace("owl", owl_namespace));
      for (int k = 0; k < equalClassElements.size(); k++) {
        Element equalClassElement = (Element) equalClassElements.get(k);
        String equalClassElementName = equalClassElement.getAttributeValue("resource", Namespace.getNamespace("rdf", rdf_namespace));

        if (equalClassElementName != null) {
          bw.write(cutString(className) + ",equivalentClass," + cutString(equalClassElementName) + "\n");
        }
      }

    }
  }

  /**
   * Method of cleaning up a string
   *
   * @param str String needed to be processed
   * @return the processed string
   */
  public String cutString(String str) {
    str = str.substring(str.indexOf("#") + 1);
    String[] strArray = str.split("(?=[A-Z])");
    str = Arrays.toString(strArray);
    return str.substring(1, str.length() - 1).replace(",", "");
  }

  @Override
  public Object execute(Object o) {
    return null;
  }

}
