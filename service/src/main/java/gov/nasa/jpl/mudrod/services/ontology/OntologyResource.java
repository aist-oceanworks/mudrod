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
package gov.nasa.jpl.mudrod.services.ontology;

import com.google.gson.Gson;
import gov.nasa.jpl.mudrod.ontology.Ontology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A ontology-driven resource for user query augmentation.
 */
@Path("/ontology")
public class OntologyResource {

  private static final Logger LOG = LoggerFactory.getLogger(OntologyResource.class);
  private Ontology ontImpl;

  public OntologyResource(@Context ServletContext sc) {
    this.ontImpl = (Ontology) sc.getAttribute("Ontology");
  }

  @GET
  @Path("/status")
  @Produces("text/html")
  public Response status() {
    return Response.ok("<h1>This is MUDROD Ontology-driven User Query Augmentation Resource: running correctly...</h1>").build();
  }

  @GET
  @Path("/synonym")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes("text/plain")
  public Response getOntologySynonyms(@QueryParam("query") String query) {
    List<String> result = new ArrayList<>();
    if (query != null) {
      Iterator<String> synonyms = ontImpl.synonyms(query);
      while (synonyms.hasNext()) {
        result.add((String) synonyms.next());
      }
    }
    String json = new Gson().toJson(result);
    LOG.info("Response received: {}", json);
    return Response.ok(json, MediaType.APPLICATION_JSON).build();
  }

  @GET
  @Path("/subclass")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes("text/plain")
  public Response getOntologySubclasses(@QueryParam("query") String query) {
    List<String> result = new ArrayList<>();
    if (query != null) {
      Iterator<String> subclasses = ontImpl.subclasses(query);
      while (subclasses.hasNext()) {
        result.add((String) subclasses.next());
      }
    }
    String json = new Gson().toJson(result);
    LOG.info("Response received: {}", json);
    return Response.ok(json, MediaType.APPLICATION_JSON).build();
  }
}
