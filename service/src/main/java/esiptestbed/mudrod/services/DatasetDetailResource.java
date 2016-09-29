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

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import esiptestbed.mudrod.main.MudrodConstants;
import esiptestbed.mudrod.main.MudrodEngine;

/**
 * Servlet implementation class DatasetDetailResource
 */
@WebServlet("/DatasetDetailResource")
public class DatasetDetailResource extends HttpServlet {

  private static final Logger LOG = LoggerFactory.getLogger(DatasetDetailResource.class);
  private static final long serialVersionUID = 1L;

  /**
   * @see HttpServlet#HttpServlet()
   */
  public DatasetDetailResource() {
    super();
  }

  /**
   * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
   *      response)
   */
  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    String shortName = request.getParameter("shortname");
    PrintWriter out = null;
    try {
      out = response.getWriter();
    } catch (IOException e) {
      LOG.error("Error whilst obtaining PrintWriter from javax.servlet.http.HttpServletResponse", e);
    }

    if (shortName != null) {
      response.setContentType("application/json");
      response.setCharacterEncoding("UTF-8");

      MudrodEngine mudrod = (MudrodEngine) request.getServletContext()
          .getAttribute("MudrodInstance");
      Properties config = mudrod.getConfig();
      String fileList = null;
      try {
        String query = "Dataset-ShortName:\"" + shortName + "\"";
        fileList = mudrod.getESDriver().searchByQuery(
            config.getProperty(MudrodConstants.ES_INDEX_NAME),
            config.getProperty(MudrodConstants.RAW_METADATA_TYPE), query, true);
      } catch (InterruptedException | ExecutionException e) {
        LOG.error("Error whilst searching for a Dataset-ShortName", e);
      }
      if (out != null) {
        out.print(fileList);
      } else {
        out.print("Please input metadata short name");
      }
      out.flush();
    }
  }

  /**
   * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
   *      response)
   */
  @Override
  protected void doPost(HttpServletRequest request,
      HttpServletResponse response) throws ServletException, IOException {
    try {
      doGet(request, response);
    } catch (ServletException | IOException e) {
      LOG.error("Error whilst executing POST!", e);
    }
  }

}
