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

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import esiptestbed.mudrod.main.MudrodConstants;
import esiptestbed.mudrod.main.MudrodEngine;

/**
 * A Dataset Detail Search Resource
 */
@Path("/datasetdetail")
public class SearchDatasetDetailResource {

  private static final Logger LOG = LoggerFactory.getLogger(SearchDatasetDetailResource.class);
  private MudrodEngine mEngine;
  
  public SearchDatasetDetailResource(@Context ServletContext sc) {
    this.mEngine = (MudrodEngine) sc.getAttribute("MudrodInstance");
  }

  @GET
  @Path("/status")
  @Produces("text/html")
  public Response status() {
    return Response
        .ok("<h1>This is MUDROD Dataset Detail Search Resource: running correctly...</h1>").build();
  }

  @POST
  @Path("{shortname}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes("text/plain")
  protected Response searchDatasetDetail(@PathParam("shortname") String shortName) {

      Properties config = mEngine.getConfig();
      String fileList = null;
      try {
        String query = "Dataset-ShortName:\"" + shortName + "\"";
        fileList = mEngine.getESDriver().searchByQuery(
            config.getProperty(MudrodConstants.ES_INDEX_NAME),
            config.getProperty(MudrodConstants.RAW_METADATA_TYPE), query, true);
      } catch (InterruptedException | ExecutionException | IOException e) {
        LOG.error("Error whilst searching for a Dataset-ShortName", e);
      }
      String json = new Gson().toJson(fileList);
      LOG.info("Response received: {}", json);
      return Response.ok(json, MediaType.APPLICATION_JSON).build();
  }

}
