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
package esiptestbed.mudrod.services;

import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.cxf.jaxrs.client.WebClient;
import org.apache.cxf.transport.http.HTTPConduit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Servlet implementation class AutoCompleteResource
 */
@Path("/autocomplete")
public class AutoCompleteResource {

  private static final Logger LOG = LoggerFactory.getLogger(AutoCompleteResource.class);
  public static final long DEFAULT_TIMEOUT = 1000000L;
  private static final String ES_URL_PROPERTY = "esiptestbed.mudrod.services.es.url";
  private URL esURL;

  /**
   * @throws MalformedURLException 
   * @see HttpServlet#HttpServlet()
   */
  public AutoCompleteResource(@Context ServletContext sc) throws MalformedURLException {
    this.esURL = new URL(sc.getInitParameter(ES_URL_PROPERTY));
  }

  @GET
  @Path("/status")
  @Produces("text/html")
  public Response status() {
    return Response
        .ok("<h1>This is MUDROD AutoCompleteResource: running correctly...</h1>").build();
  }

  @POST
  @Path("/complete")
  @Produces("application/json")
  public Response forwardES(InputStream is,
      @HeaderParam("Content-Disposition") String contentDisposition) {
    return forwardProxy(is, esURL.toString(), contentDisposition);
  }

  private Response forwardProxy(InputStream is, String url,
      String contentDisposition) {
    LOG.info("POST'ing query [{}] for to AutoCompletion at endpoint:[{}]", contentDisposition, url);
    WebClient client = WebClient.create(url).accept("application/json")
        .header("Content-Disposition", contentDisposition);
    HTTPConduit conduit = WebClient.getConfig(client).getHttpConduit();
    conduit.getClient().setConnectionTimeout(DEFAULT_TIMEOUT);
    conduit.getClient().setReceiveTimeout(DEFAULT_TIMEOUT);
    Response response = client.post(is);
    String json = response.readEntity(String.class);
    LOG.info("Response received: {}", json);
    return Response.ok(json, MediaType.APPLICATION_JSON).build();
  }

}
