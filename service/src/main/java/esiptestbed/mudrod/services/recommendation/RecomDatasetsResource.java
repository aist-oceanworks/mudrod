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
package esiptestbed.mudrod.services.recommendation;

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

import com.google.gson.JsonObject;

import esiptestbed.mudrod.main.MudrodEngine;
import esiptestbed.mudrod.recommendation.structure.RecommendationData;

/**
 * A Dataset recommendation resource.
 */
@Path("/recommendation")
public class RecomDatasetsResource {

  private MudrodEngine mEngine;

  public RecomDatasetsResource(@Context ServletContext sc) {
    this.mEngine = (MudrodEngine) sc.getAttribute("MudrodInstance");
  }

  @GET
  @Path("/status")
  @Produces("text/html")
  public Response status() {
    return Response
        .ok("<h1>This is MUDROD Recommendation Datasets Resource: running correctly...</h1>").build();
  }

  @PUT
  @Path("{shortname}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes("text/plain")
  public Response hybridRecommendation(@PathParam("shortname") String shortName) {
    JsonObject json = new JsonObject();
    if (shortName != null) {
      RecommendationData recom = new RecommendationData(mEngine.getConfig(), mEngine.getESDriver(), null);
      json = new JsonObject();
      json.add("RecommendationData", recom.getRecomDataInJson(shortName, 10));
    }
    return Response.ok(json.toString(), MediaType.APPLICATION_JSON).build();
  }
}
