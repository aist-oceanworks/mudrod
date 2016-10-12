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
package esiptestbed.mudrod.services.ontology;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import esiptestbed.mudrod.main.MudrodEngine;
import esiptestbed.mudrod.ontology.OntologyFactory;

/**
 * A ontology-driven resource for user query augmentation.
 */
@Path("/ontology")
public class OntologyResource {

  private static final Logger LOG = LoggerFactory.getLogger(OntologyResource.class);
  private MudrodEngine mEngine;

  public OntologyResource(@Context ServletContext sc) {
    this.mEngine = (MudrodEngine) sc.getAttribute("MudrodInstance");
  }

  @GET
  @Path("/status")
  @Produces("text/html")
  public Response status() {
    return Response
        .ok("<h1>This is MUDROD Ontology-driven User Query Augmentation Resource: running correctly...</h1>").build();
  }

  @PUT
  @Path("/synonym/{term}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes("text/plain")
  public Response getOntologySynonyms(@PathParam("term") String term) {
    List<String> result = new ArrayList<>();
    if (term != null) {
      OntologyFactory ontFactory = new OntologyFactory(mEngine.getConfig());
      Iterator<String> synonyms = ontFactory.getOntology().synonyms(term);
      while (synonyms.hasNext()) {
        result.add((String) synonyms.next());
      }
    }
    String json = new Gson().toJson(result);
    LOG.info("Response received: {}", json);
    return Response.ok(json, MediaType.APPLICATION_JSON).build();
  }

  @PUT
  @Path("/subclasses/{term}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes("text/plain")
  public Response getOntologySubclasses(@PathParam("term") String term) {
    List<String> result = new ArrayList<>();
    if (term != null) {
      OntologyFactory ontFactory = new OntologyFactory(mEngine.getConfig());
      Iterator<String> subclasses = ontFactory.getOntology().subclasses(term);
      while (subclasses.hasNext()) {
        result.add((String) subclasses.next());
      }
    }
    String json = new Gson().toJson(result);
    LOG.info("Response received: {}", json);
    return Response.ok(json, MediaType.APPLICATION_JSON).build();
  }
}
