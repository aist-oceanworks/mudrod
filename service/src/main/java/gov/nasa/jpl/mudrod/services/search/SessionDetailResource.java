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
package gov.nasa.jpl.mudrod.services.search;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import gov.nasa.jpl.mudrod.main.MudrodConstants;
import gov.nasa.jpl.mudrod.main.MudrodEngine;
import gov.nasa.jpl.mudrod.weblog.structure.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContext;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Servlet implementation class SessionDetail
 */
@Path("/sessiondetail")
public class SessionDetailResource {

  private static final Logger LOG = LoggerFactory.getLogger(SessionDetailResource.class);
  private MudrodEngine mEngine;

  /**
   * Constructor
   *
   * @param sc an instantiated {@link javax.servlet.ServletContext}
   */
  public SessionDetailResource(@Context ServletContext sc) {
    this.mEngine = (MudrodEngine) sc.getAttribute("MudrodInstance");
  }

  @GET
  @Path("/status")
  @Produces("text/html")
  public Response status() {
    return Response.ok("<h1>This is MUDROD Session Detail Search Resource: running correctly...</h1>").build();
  }

  @POST
  @Path("{CleanupType}-{SessionID}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes("text/plain")
  protected Response searchSessionDetail(@PathParam("CleanupType") String cleanupType, @PathParam("SessionID") String sessionID) {

    JsonObject json = new JsonObject();
    if (sessionID != null) {
      Session session = new Session(mEngine.getConfig(), mEngine.getESDriver());
      json = session.getSessionDetail(mEngine.getConfig().getProperty(MudrodConstants.ES_INDEX_NAME, "mudrod"), cleanupType, sessionID);
    }
    LOG.info("Response received: {}", json);
    return Response.ok(new Gson().toJson(json), MediaType.APPLICATION_JSON).build();
  }
}
