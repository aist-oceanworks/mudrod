package esiptestbed.mudrod.webservlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import esiptestbed.mudrod.integration.LinkageIntegration;
import esiptestbed.mudrod.main.MudrodEngine;

/**
 * Servlet implementation class SearchMetadata
 */
@WebServlet("/SearchMetadata")
public class SearchMetadata extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public SearchMetadata() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		response.setContentType("application/json");  
		response.setCharacterEncoding("UTF-8");
		String query = request.getParameter("query");
		
		//MudrodEngine mudrod = new MudrodEngine();
		MudrodEngine mudrod = (MudrodEngine) request.getServletContext().getAttribute("MudrodInstance");
		Map<String, String> config= mudrod.getConfig();
		String fileList=null;
		try {
			LinkageIntegration li = new LinkageIntegration(config, mudrod.getES(), null);
			String semantic_query = li.getModifiedQuery(query, 3);
			fileList = mudrod.getES().searchByQuery(config.get("indexName"), config.get("raw_metadataType"), semantic_query);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		PrintWriter out = response.getWriter();
		out.print(fileList); 
    	out.flush();
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
	}

}
