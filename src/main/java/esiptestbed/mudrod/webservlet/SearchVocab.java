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
package esiptestbed.mudrod.webservlet;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.gson.JsonObject;

import esiptestbed.mudrod.integration.LinkageIntegration;
import esiptestbed.mudrod.main.MudrodEngine;

/**
 * Servlet implementation class SearchVocab
 */
@WebServlet("/SearchVocab")
public class SearchVocab extends HttpServlet {
  private static final long serialVersionUID = 1L;

  /**
   * @see HttpServlet#HttpServlet()
   */
  public SearchVocab() {
    super();
    // TODO Auto-generated constructor stub
  }

  /**
   * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
   *      response)
   */
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    // TODO Auto-generated method stub
    String concept = request.getParameter("concept");
    PrintWriter out = response.getWriter();

    if (concept != null) {
      response.setContentType("application/json");
      response.setCharacterEncoding("UTF-8");
      // MudrodEngine mudrod = new MudrodEngine();
      MudrodEngine mudrod = (MudrodEngine) request.getServletContext()
          .getAttribute("MudrodInstance");
      LinkageIntegration li = new LinkageIntegration(mudrod.getConfig(),
          mudrod.getES(), null);

      JsonObject json_kb = new JsonObject();
      JsonObject json_graph = new JsonObject();

      json_graph = li.getIngeratedListInJson(concept, 10);

      json_kb.add("graph", json_graph);
      out.print(json_kb.toString());
      out.flush();
    } else {
      out.print("Please input query");
      out.flush();
    }
  }

  /**
   * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
   *      response)
   */
  protected void doPost(HttpServletRequest request,
      HttpServletResponse response) throws ServletException, IOException {
    // TODO Auto-generated method stub
  }

}
