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


import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;

import esiptestbed.mudrod.main.MudrodEngine;
import esiptestbed.mudrod.weblog.structure.Session;

/**
 * Servlet implementation class SessionDetail
 */
@WebServlet("/SessionDetail")
public class SessionDetailResource extends HttpServlet {
  
  private static final Logger LOG = LoggerFactory.getLogger(SessionDetailResource.class);
  private static final long serialVersionUID = 1L;

  /**
   * Default constructor. 
   */
  public SessionDetailResource() {
    //default constructor
  }

  /**
   * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
   */
  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    String sessionID = request.getParameter("SessionID");
    String cleanupType = request.getParameter("CleanupType");
    PrintWriter out = null;
    try {
      out = response.getWriter();
    } catch (IOException e) {
      LOG.error("Error whilst obtaining the HttpServletResponse PrintWriter.", e);
    }

    if(sessionID!=null) {
      response.setContentType("application/json");  
      response.setCharacterEncoding("UTF-8");

      MudrodEngine mudrod = (MudrodEngine) request.getServletContext()
          .getAttribute("MudrodInstance");

      Session session = new Session(mudrod.getConfig(), mudrod.getESDriver());
      JsonObject json = session.getSessionDetail(cleanupType, sessionID);

      out.print(json.toString());
      out.flush();
    } else {
      out.print("Please input SessionID");
      out.flush();
    }
  }

  /**
   * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
   */
  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    try {
      doGet(request, response);
    } catch (ServletException | IOException e) {
      LOG.error("There was an error whilst POST'ing the Session Detail.", e);
    }
  }
}
