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

import gov.nasa.jpl.mudrod.main.MudrodConstants;
import gov.nasa.jpl.mudrod.main.MudrodEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContext;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

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

  /**
   * A simple health status checker for this resource.
   *
   * @return a static html response if the service is running correctly.
   */
  @GET
  @Path("/status")
  @Produces("text/html")
  public Response status() {
    return Response.ok("<h1>This is MUDROD Dataset Detail Search Resource: running correctly...</h1>").build();
  }

  @GET
  @Path("/search")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes("text/plain")
  public Response searchDatasetDetail(@QueryParam("shortname") String shortName) {
    Properties config = mEngine.getConfig();
    String dataDetailJson = null;
    try {
      String query = "Dataset-ShortName:\"" + shortName + "\"";
      dataDetailJson = mEngine.getESDriver().searchByQuery(config.getProperty(MudrodConstants.ES_INDEX_NAME), config.getProperty(MudrodConstants.RAW_METADATA_TYPE), query, true);
    } catch (InterruptedException | ExecutionException | IOException e) {
      LOG.error("Error whilst searching for a Dataset-ShortName: ", e);
    }
    LOG.info("Response received: {}", dataDetailJson);
    return Response.ok(dataDetailJson, MediaType.APPLICATION_JSON).build();
  }
}
