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

import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import com.google.gson.JsonObject;

import esiptestbed.mudrod.integration.LinkageIntegration;
import esiptestbed.mudrod.main.MudrodEngine;
import esiptestbed.mudrod.metadata.structure.PODAACMetadata;
import esiptestbed.mudrod.weblog.structure.Session;

/**
 * Servlet implementation class DatasetDetail
 */
@WebServlet("/DatasetDetail")
public class DatasetDetail extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public DatasetDetail() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		// TODO Auto-generated method stub
		String shortName = request.getParameter("shortname");
		PrintWriter out = null;
		try {
			out = response.getWriter();
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (shortName != null) {
			response.setContentType("application/json");
			response.setCharacterEncoding("UTF-8");

			MudrodEngine mudrod = (MudrodEngine) request.getServletContext().getAttribute("MudrodInstance");
			Map<String, String> config = mudrod.getConfig();
			String fileList = null;
			try {
				String query = "Dataset-ShortName:\"" + shortName + "\"";
				fileList = mudrod.getES().searchByQuery(config.get("indexName"), config.get("raw_metadataType"),
						query, true);
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
			out = response.getWriter();
			out.print(fileList);
			out.flush();
		} else {
			out.print("Please input metadata short name");
			out.flush();
		}
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}
