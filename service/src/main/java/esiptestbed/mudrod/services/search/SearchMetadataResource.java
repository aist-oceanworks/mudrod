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

import java.util.Properties;

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

import com.google.gson.Gson;

import esiptestbed.mudrod.main.MudrodConstants;
import esiptestbed.mudrod.main.MudrodEngine;
import esiptestbed.mudrod.ssearch.Ranker;
import esiptestbed.mudrod.ssearch.Searcher;

/**
 * A Mudrod Metadata Search Resource
 */
@Path("/metadata")
public class SearchMetadataResource {

  private static final Logger LOG = LoggerFactory.getLogger(SearchMetadataResource.class);

  private MudrodEngine mEngine;
  private Searcher searcher;
  private Ranker ranker;

  public SearchMetadataResource(@Context ServletContext sc) {
    this.mEngine = (MudrodEngine) sc.getAttribute("MudrodInstance");
    this.searcher = (Searcher) sc.getAttribute("MudrodSearcher");
    this.ranker = (Ranker) sc.getAttribute("MudrodRanker");
  }

  @GET
  @Path("/status")
  @Produces("text/html")
  public Response status() {
    return Response
        .ok("<h1>This is MUDROD Metadata Search Resource: running correctly...</h1>").build();
  }

  @GET
  @Path("/search")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes("text/plain")
  public Response searchMetadata(@QueryParam("query") String query, @QueryParam("operator") String operator) {
    Properties config = mEngine.getConfig();
    String fileList = searcher.ssearch(config.getProperty(MudrodConstants.ES_INDEX_NAME),
        config.getProperty(MudrodConstants.RAW_METADATA_TYPE), 
        query,
        operator, //please replace it with and, or, phrase
        ranker);
    String json = new Gson().toJson(fileList);
    LOG.info("Response received: {}", json);
    return Response.ok(json, MediaType.APPLICATION_JSON).build();
  }

}
