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
import java.io.PrintWriter;
import java.util.Properties;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import esiptestbed.mudrod.main.MudrodConstants;
import esiptestbed.mudrod.main.MudrodEngine;
import esiptestbed.mudrod.ssearch.Ranker;
import esiptestbed.mudrod.ssearch.Searcher;

/**
 * Servlet implementation class SearchMetadataResource
 */
public class SearchMetadataResource extends HttpServlet {
  private static final long serialVersionUID = 1L;

  /**
   * @see HttpServlet#HttpServlet()
   */
  public SearchMetadataResource() {
    super();
  }

  /**
   * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
   *      response)
   */
  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    response.setContentType("application/json");
    response.setCharacterEncoding("UTF-8");
    String query = request.getParameter("query");
    String operator = request.getParameter("operator").toLowerCase();

    MudrodEngine mudrod = (MudrodEngine) request.getServletContext()
        .getAttribute("MudrodInstance");
    Searcher sr = (Searcher) request.getServletContext()
        .getAttribute("MudrodSearcher");
    Ranker rr = (Ranker) request.getServletContext()
        .getAttribute("MudrodRanker");
    Properties config = mudrod.getConfig();
    String fileList = null;

    fileList = sr.ssearch(config.getProperty(MudrodConstants.ES_INDEX_NAME),
        config.getProperty(MudrodConstants.RAW_METADATA_TYPE), 
        query,
        operator, //please replace it with and, or, phrase
        rr);

    PrintWriter out = response.getWriter();
    out.print(fileList);
    out.flush();
  }

  /**
   * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
   *      response)
   */
  @Override
  protected void doPost(HttpServletRequest request,
      HttpServletResponse response) throws ServletException, IOException {
  }

}
