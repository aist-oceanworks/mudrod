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
package gov.nasa.jpl.mudrod.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * ClassName: HttpRequest
 * Function: Http request tool.
 */
public class HttpRequest {

  private static final Logger LOG = LoggerFactory.getLogger(HttpRequest.class);

  public HttpRequest() {
  }

  public String getRequest(String requestUrl) {
    String line = null;
    try {
      URL url = new URL(requestUrl);

      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setDoOutput(true);

      connection.setConnectTimeout(5000);
      connection.setReadTimeout(5000);
      int code = connection.getResponseCode();
      if (code != HttpURLConnection.HTTP_OK) {
        line = "{\"exception\":\"Service failed\"}";
        LOG.info(line);
      } else {
        InputStream content = connection.getInputStream();
        BufferedReader in = new BufferedReader(new InputStreamReader(content));
        line = in.readLine();
      }
    } catch (Exception e) {
      line = "{\"exception\":\"No service was found\"}";
      LOG.error(line);
    }
    return line;
  }
}
