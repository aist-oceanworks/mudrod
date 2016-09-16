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

import esiptestbed.mudrod.main.MudrodEngine;
import esiptestbed.mudrod.weblog.structure.Session;

/**
 * Servlet implementation class SessionDetail
 */
@WebServlet("/SessionDetail")
public class SessionDetail extends HttpServlet {
  private static final long serialVersionUID = 1L;

  /**
   * Default constructor. 
   */
  public SessionDetail() {
  }

  /**
   * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
   */
  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    String sessionID = request.getParameter("SessionID");
    String cleanupType = request.getParameter("CleanupType");
    PrintWriter out = null;
    try {
      out = response.getWriter();
    } catch (IOException e) {
      e.printStackTrace();
    }

    if(sessionID!=null)
    {
      response.setContentType("application/json");  
      response.setCharacterEncoding("UTF-8");

      MudrodEngine mudrod = (MudrodEngine) request.getServletContext()
          .getAttribute("MudrodInstance");

      Session session = new Session(mudrod.getConfig(), mudrod.getESDriver());
      JsonObject json = session.getSessionDetail(cleanupType, sessionID);

      out.print(json.toString());
      out.flush();
    }else{
      out.print("Please input SessionID");
      out.flush();
    }
  }

  /**
   * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
   */
  protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    doGet(request, response);
  }
}
