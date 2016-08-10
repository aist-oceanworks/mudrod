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
		String SessionID = request.getParameter("SessionID");
		String cleanupType = request.getParameter("CleanupType");
		PrintWriter out = null;
	    try {
	      out = response.getWriter();
	    } catch (IOException e) {
	      e.printStackTrace();
	    }
	    
		if(SessionID!=null)
		{
			response.setContentType("application/json");  
			response.setCharacterEncoding("UTF-8");

			MudrodEngine mudrod = (MudrodEngine) request.getServletContext()
			          .getAttribute("MudrodInstance");
			
			Session session = new Session(mudrod.getConfig(),mudrod.getES());
			JsonObject json = session.getSessionDetail(cleanupType,  SessionID);

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
