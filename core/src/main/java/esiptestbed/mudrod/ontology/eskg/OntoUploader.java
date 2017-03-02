package esiptestbed.mudrod.ontology.eskg;

import java.io.*;
import java.lang.Exception;
import java.lang.String;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class OntoUploader {
    static final String REST_URL = "http://semanticportal.esipfed.org";
    private static final String API_KEY = "";


	public OntoUploader() {
		// TODO Auto-generated constructor stub
	}
	
    private static String postJSON(String urlToGet, String body) {
        URL url;
        HttpURLConnection conn;

        String line;
        String result = "";
        try {
            url = new URL(urlToGet);
            conn = (HttpURLConnection) url.openConnection();
            conn.setDoOutput(true);
            conn.setDoInput(true);
            conn.setInstanceFollowRedirects(false);
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Authorization", "apikey token=" + API_KEY);
            conn.setRequestProperty("Accept", "application/json");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("charset", "utf-8");
            conn.setUseCaches(false);

            DataOutputStream wr = new DataOutputStream(conn.getOutputStream());
            wr.write(body.getBytes());
            wr.flush();
            wr.close();
            conn.disconnect();

            InputStream is;
            boolean error = false;
            if (conn.getResponseCode() >= 200 && conn.getResponseCode() < 400) {
                is = conn.getInputStream();
            } else {
                error = true;
                is = conn.getErrorStream();
            }

            BufferedReader rd = new BufferedReader(new InputStreamReader(is));
            while ((line = rd.readLine()) != null) {
                result += line;
            }
            rd.close();

            if (error) throw new Exception(result);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;
    }

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
