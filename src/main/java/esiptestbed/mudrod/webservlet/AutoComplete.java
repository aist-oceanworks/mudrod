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
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.gson.Gson;

import esiptestbed.mudrod.main.MudrodEngine;
import esiptestbed.mudrod.webservlet.structure.AutoCompleteData;

/**
 * Servlet implementation class AutoComplete
 */
@WebServlet("/AutoComplete")
public class AutoComplete extends HttpServlet {
  private static final long serialVersionUID = 1L;

  /**
   * @see HttpServlet#HttpServlet()
   */
  public AutoComplete() {
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
  }

  /**
   * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
   *      response)
   */
  protected void doPost(HttpServletRequest request,
      HttpServletResponse response) throws ServletException, IOException {
    // TODO Auto-generated method stub
    response.setContentType("application/json");
    response.setCharacterEncoding("UTF-8");
    String chars = request.getParameter("chars");

    List<AutoCompleteData> result = new ArrayList<AutoCompleteData>();
    // MudrodEngine mudrod = new MudrodEngine();
    MudrodEngine mudrod = (MudrodEngine) request.getServletContext()
        .getAttribute("MudrodInstance");
    List<String> suggestList = mudrod.getES()
        .autoComplete(mudrod.getConfig().get("indexName"), chars);
    for (final String item : suggestList) {
      result.add(new AutoCompleteData(item, item));
    }
    response.getWriter().write(new Gson().toJson(result));
  }

}
