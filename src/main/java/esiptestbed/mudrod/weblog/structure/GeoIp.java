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
package esiptestbed.mudrod.weblog.structure;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import esiptestbed.mudrod.utils.HttpRequest;

public class GeoIp {

  public Coordinates toLocation(String ip) {
    String url = "http://getcitydetails.geobytes.com/GetCityDetails?fqcn=" + ip;
    HttpRequest http = new HttpRequest();
    String response = http.getRequest(url);
    JsonParser parser = new JsonParser();
    JsonElement jobSon = parser.parse(response);
    JsonObject responseObject = jobSon.getAsJsonObject();

    Coordinates co = new Coordinates();
    String lon = responseObject.get("geobyteslongitude").toString()
        .replace("\"", "");
    String lat = responseObject.get("geobyteslatitude").toString().replace("\"",
        "");
    co.latlon = lat + "," + lon;
    return co;
  }
}
