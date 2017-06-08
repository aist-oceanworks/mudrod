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

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test cases for {@link gov.nasa.jpl.mudrod.ontology.process.LocalOntology}
 */
public class TestLocalOntology {

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  private LocalOntology lOnt;

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    lOnt = new LocalOntology();
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    lOnt = null;
  }

  /**
   * Test method for {@link gov.nasa.jpl.mudrod.ontology.process.LocalOntology#LocalOntology()}.
   */
  @Test
  public final void testLocalOntology() {
    assertNotNull("Test setUp should create a new instance of LocalOntology.", lOnt);
  }

  /**
   * Test method for {@link gov.nasa.jpl.mudrod.ontology.process.LocalOntology#getInstance()}.
   */
  @Test
  public final void testGetInstance() {
    assertSame("Ontology instance should be of type LocalOntology", LocalOntology.class, lOnt.getInstance().getClass());
  }

  /**
   * Test method for {@link gov.nasa.jpl.mudrod.ontology.process.LocalOntology#load()}.
   * @throws IOException 
   */
  @Ignore
  @Test
  public final void testLoad() throws IOException {
    lOnt.load();
    assertTrue("Resource list should have a minimum of one resource.", lOnt.getLoadedOntologyResources().size() == 1);
  }

  /**
   * Test method for {@link gov.nasa.jpl.mudrod.ontology.process.LocalOntology#load(java.lang.String[])}.
   */
  @Ignore
  @Test
  public final void testLoadStringArray() {
    fail("Not yet implemented"); // TODO
  }

  /**
   * Test method for {@link gov.nasa.jpl.mudrod.ontology.process.LocalOntology#getParser()}.
   */
  @Ignore
  @Test
  public final void testGetParser() {
    fail("Not yet implemented"); // TODO
  }

  /**
   * Test method for {@link gov.nasa.jpl.mudrod.ontology.process.LocalOntology#getModel()}.
   */
  @Ignore
  @Test
  public final void testGetModel() {
    fail("Not yet implemented"); // TODO
  }

  /**
   * Test method for {@link gov.nasa.jpl.mudrod.ontology.process.LocalOntology#merge(gov.nasa.jpl.mudrod.ontology.Ontology)}.
   */
  @Ignore
  @Test
  public final void testMerge() {
    fail("Not yet implemented"); // TODO
  }

  /**
   * Test method for {@link gov.nasa.jpl.mudrod.ontology.process.LocalOntology#subclasses(java.lang.String)}.
   */
  @Ignore
  @Test
  public final void testSubclasses() {
    fail("Not yet implemented"); // TODO
  }

  /**
   * Test method for {@link gov.nasa.jpl.mudrod.ontology.process.LocalOntology#synonyms(java.lang.String)}.
   */
  @Ignore
  @Test
  public final void testSynonyms() {
    fail("Not yet implemented"); // TODO
  }

  /**
   * Test method for {@link gov.nasa.jpl.mudrod.ontology.process.LocalOntology#addSearchTerm(java.lang.String, org.apache.jena.ontology.OntResource)}.
   */
  @Ignore
  @Test
  public final void testAddSearchTerm() {
    fail("Not yet implemented"); // TODO
  }

  /**
   * Test method for {@link gov.nasa.jpl.mudrod.ontology.process.LocalOntology#retrieve(java.lang.String)}.
   */
  @Ignore
  @Test
  public final void testRetrieve() {
    fail("Not yet implemented"); // TODO
  }

  /**
   * Test method for {@link gov.nasa.jpl.mudrod.ontology.process.LocalOntology#renderHierarchy(java.io.PrintStream, org.apache.jena.ontology.OntClass, java.util.List, int)}.
   */
  @Ignore
  @Test
  public final void testRenderHierarchy() {
    fail("Not yet implemented"); // TODO
  }

  /**
   * Test method for {@link gov.nasa.jpl.mudrod.ontology.process.LocalOntology#renderClassDescription(java.io.PrintStream, org.apache.jena.ontology.OntClass, int)}.
   */
  @Ignore
  @Test
  public final void testRenderClassDescription() {
    fail("Not yet implemented"); // TODO
  }

  /**
   * Test method for {@link gov.nasa.jpl.mudrod.ontology.process.LocalOntology#renderRestriction(java.io.PrintStream, org.apache.jena.ontology.Restriction)}.
   */
  @Ignore
  @Test
  public final void testRenderRestriction() {
    fail("Not yet implemented"); // TODO
  }

  /**
   * Test method for {@link gov.nasa.jpl.mudrod.ontology.process.LocalOntology#renderURI(java.io.PrintStream, org.apache.jena.shared.PrefixMapping, java.lang.String)}.
   */
  @Ignore
  @Test
  public final void testRenderURI() {
    fail("Not yet implemented"); // TODO
  }

  /**
   * Test method for {@link gov.nasa.jpl.mudrod.ontology.process.LocalOntology#renderAnonymous(java.io.PrintStream, org.apache.jena.rdf.model.Resource, java.lang.String)}.
   */
  @Ignore
  @Test
  public final void testRenderAnonymous() {
    fail("Not yet implemented"); // TODO
  }

  /**
   * Test method for {@link gov.nasa.jpl.mudrod.ontology.process.LocalOntology#indent(java.io.PrintStream, int)}.
   */
  @Ignore
  @Test
  public final void testIndent() {
    fail("Not yet implemented"); // TODO
  }

  /**
   * Test method for {@link gov.nasa.jpl.mudrod.ontology.process.LocalOntology#main(java.lang.String[])}.
   */
  @Ignore
  @Test
  public final void testMain() {
    fail("Not yet implemented"); // TODO
  }

}
