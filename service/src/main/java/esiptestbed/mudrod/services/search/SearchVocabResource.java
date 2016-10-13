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
package esiptestbed.mudrod.services.search;

import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;

import esiptestbed.mudrod.integration.LinkageIntegration;
import esiptestbed.mudrod.main.MudrodEngine;

/**
 * A Mudrod Search Vocabulary Resource
 */
@Path("/vocabulary")
public class SearchVocabResource {

  private static final Logger LOG = LoggerFactory.getLogger(SearchMetadataResource.class);

  private MudrodEngine mEngine;

  public SearchVocabResource(@Context ServletContext sc) {
    this.mEngine = (MudrodEngine) sc.getAttribute("MudrodInstance");
  }

  @GET
  @Path("/status")
  @Produces("text/html")
  public Response status() {
    return Response
        .ok("<h1>This is MUDROD Vocabulary Search Resource: running correctly...</h1>").build();
  }

  @GET
  @Path("/search")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes("text/plain")
  public Response searchVocabulary(@QueryParam("query") String concept) {
    JsonObject json = new JsonObject();
    if (concept != null) {
      LinkageIntegration li = new LinkageIntegration(mEngine.getConfig(),
          mEngine.getESDriver(), null);
      json = new JsonObject();
      json.add("graph", li.getIngeratedListInJson(concept));
    }
    LOG.info("Response received: {}", json);
    return Response.ok(json, MediaType.APPLICATION_JSON).build();
  }

}
