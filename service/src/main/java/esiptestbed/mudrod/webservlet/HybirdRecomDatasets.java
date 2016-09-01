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
import esiptestbed.mudrod.recommendation.structure.HybirdRecommendation;

/**
 * Servlet implementation class RecomDatasets
 */
@WebServlet("/HybirdRecomDatasets")
public class HybirdRecomDatasets extends HttpServlet {
  private static final long serialVersionUID = 1L;

  /**
   * @see HttpServlet#HttpServlet()
   */
  public HybirdRecomDatasets() {
    super();
    // TODO Auto-generated constructor stub
  }

  /**
   * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
   *      response)
   */
  @Override
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

      MudrodEngine mudrod = (MudrodEngine) request.getServletContext()
          .getAttribute("MudrodInstance");
      HybirdRecommendation recom = new HybirdRecommendation(mudrod.getConfig(),
          mudrod.getES(), null);
      JsonObject jsonKb = new JsonObject();
      jsonKb.add("recomdata", recom.getRecomDataInJson(shortName, 10));
      out.print(jsonKb.toString());
      out.flush();
    } else {
      out.print("Please input metadata short name");
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
    // TODO Auto-generated method stub
    doGet(request, response);
  }

}
