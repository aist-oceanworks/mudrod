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
package gov.nasa.jpl.mudrod.weblog.structure;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ClassName: RequestUrl Function: request url relate operations
 */
public class RequestUrl {

  private static final Logger LOG = LoggerFactory.getLogger(RequestUrl.class);

  /**
   * Default Constructor
   */
  public RequestUrl() {
    /* Default Constructor */
  }

  /**
   * UrlPage: Get url page from url link
   *
   * @param strURL request url
   * @return page name
   */
  public static String urlPage(String strURL) {
    String strPage = null;
    String[] arrSplit = null;

    String newURL = strURL.trim().toLowerCase();

    arrSplit = newURL.split("[?]");
    if (newURL.length() > 0 && arrSplit.length > 1 && arrSplit[0] != null) {
      strPage = arrSplit[0];
    }

    return strPage;
  }

  /**
   * TruncateUrlPage: Get url params from url link
   *
   * @param strURL
   * @return url params
   */
  private static String truncateUrlPage(String strURL) {
    String strAllParam = null;
    String[] arrSplit = null;

    strURL = strURL.trim().toLowerCase(); // keep this in mind

    arrSplit = strURL.split("[?]");
    if (strURL.length() > 1) {
      if (arrSplit.length > 1) {
        if (arrSplit[1] != null) {
          strAllParam = arrSplit[1];
        }
      }
    }

    return strAllParam;
  }

  /**
   * URLRequest: Get url params from url link in a map format
   *
   * @param URL request url
   * @return url params key value map
   */
  public static Map<String, String> uRLRequest(String URL) {
    Map<String, String> mapRequest = new HashMap<String, String>();

    String[] arrSplit = null;

    String strUrlParam = truncateUrlPage(URL);
    if (strUrlParam == null) {
      return mapRequest;
    }

    arrSplit = strUrlParam.split("[&]");
    for (String strSplit : arrSplit) {
      String[] arrSplitEqual = null;
      arrSplitEqual = strSplit.split("[=]");

      if (arrSplitEqual.length > 1) {

        mapRequest.put(arrSplitEqual[0], arrSplitEqual[1]);

      } else {
        if (arrSplitEqual[0] != "") {

          mapRequest.put(arrSplitEqual[0], "");
        }
      }
    }
    return mapRequest;
  }

  /**
   * GetSearchInfo: Get search information from url link
   *
   * @param URL request url
   * @return search params
   * @throws UnsupportedEncodingException UnsupportedEncodingException
   */
  public String getSearchInfo(String URL) throws UnsupportedEncodingException {
    List<String> info = new ArrayList<String>();
    String keyword = "";
    Map<String, String> mapRequest = RequestUrl.uRLRequest(URL);
    if (mapRequest.get("search") != null) {
      try {
        keyword = mapRequest.get("search");

        keyword = URLDecoder.decode(keyword.replaceAll("%(?![0-9a-fA-F]{2})", "%25"), "UTF-8");
        if (keyword.contains("%2b") || keyword.contains("%20") || keyword.contains("%25")) {
          keyword = keyword.replace("%2b", " ");
          keyword = keyword.replace("%20", " ");
          keyword = keyword.replace("%25", " ");
        }

        keyword = keyword.replaceAll("[-+^:,*_\"]", " ").replace("\\", " ").replaceAll("\\s+", " ").trim();

      } catch (UnsupportedEncodingException e) {
        LOG.error(mapRequest.get("search"));
        e.printStackTrace();
      }
      if (!"".equals(keyword)) {
        info.add(keyword);
      }

    }

    if (mapRequest.get("ids") != null && mapRequest.get("values") != null) {
      String id_raw = URLDecoder.decode(mapRequest.get("ids"), "UTF-8");
      String value_raw = URLDecoder.decode(mapRequest.get("values"), "UTF-8");
      String[] ids = id_raw.split(":");
      String[] values = value_raw.split(":");

      int a = ids.length;
      int b = values.length;
      int l = a < b ? a : b;

      for (int i = 0; i < l; i++) {
        if (ids[i].equals("collections") || ids[i].equals("measurement") || ids[i].equals("sensor") || ids[i].equals("platform") || ids[i].equals("variable") || ids[i].equals("spatialcoverage")) {
          try {
            values[i] = values[i].replaceAll("%(?![0-9a-fA-F]{2})", "%25");
            if (!URLDecoder.decode(values[i], "UTF-8").equals(keyword) && !URLDecoder.decode(values[i], "UTF-8").equals("")) {
              String item = URLDecoder.decode(values[i], "UTF-8").trim();
              if (item.contains("%2b") || item.contains("%20") || item.contains("%25")) {
                item = item.replace("%2b", " ");
                item = item.replace("%20", " ");
                item = item.replace("%25", " ");
              }
              item = item.replaceAll("[-+^:,*_\"]", " ").replace("\\", " ").replaceAll("\\s+", " ").trim();
              info.add(item);
            }
          } catch (Exception e) {
            LOG.error(values[i]);
            e.printStackTrace();
          }
        }

      }
    }

    return String.join(",", info);
  }

  /**
   * GetSearchWord: Get search words from url link
   *
   * @param url request url
   * @return query
   */
  public static String getSearchWord(String url) {
    String keyword = "";

    Map<String, String> mapRequest = RequestUrl.uRLRequest(url);
    if (mapRequest.get("search") != null) {
      try {
        keyword = mapRequest.get("search");

        keyword = URLDecoder.decode(keyword.replaceAll("%(?![0-9a-fA-F]{2})", "%25"), "UTF-8");
        if (keyword.contains("%2b") || keyword.contains("%20") || keyword.contains("%25")) {
          keyword = keyword.replace("%2b", " ");
          keyword = keyword.replace("%20", " ");
          keyword = keyword.replace("%25", " ");
        }
        keyword = keyword.replaceAll("[-+^:,*_\"]", " ").replace("\\", " ").replaceAll("\\s+", " ").trim();
      } catch (UnsupportedEncodingException e) {
        LOG.error(mapRequest.get("search"));
        e.printStackTrace();
      }
    }

    return keyword;
  }

  /**
   * GetFilterInfo: Get filter params from url link
   *
   * @param url request url
   * @return filter facet key pair map
   * @throws UnsupportedEncodingException UnsupportedEncodingException
   */
  public static Map<String, String> getFilterInfo(String url) throws UnsupportedEncodingException {
    List<String> info = new ArrayList<>();
    Map<String, String> filterValues = new HashMap<>();

    String keyword = "";
    Map<String, String> mapRequest = RequestUrl.uRLRequest(url);
    if (mapRequest.get("search") != null) {
      try {
        keyword = mapRequest.get("search");

        keyword = URLDecoder.decode(keyword.replaceAll("%(?![0-9a-fA-F]{2})", "%25"), "UTF-8");
        if (keyword.contains("%2b") || keyword.contains("%20") || keyword.contains("%25")) {
          keyword = keyword.replace("%2b", " ");
          keyword = keyword.replace("%20", " ");
          keyword = keyword.replace("%25", " ");
        }
        keyword = keyword.replaceAll("[-+^:,*_\"]", " ").replace("\\", " ").replaceAll("\\s+", " ").trim();

      } catch (UnsupportedEncodingException e) {
        LOG.error(mapRequest.get("search"));
        e.printStackTrace();
      }
      if (!"".equals(keyword)) {
        info.add(keyword);
      }

    }

    if (mapRequest.get("ids") != null && mapRequest.get("values") != null) {
      String idRaw = URLDecoder.decode(mapRequest.get("ids"), "UTF-8");
      String valueRaw = URLDecoder.decode(mapRequest.get("values"), "UTF-8");
      String[] ids = idRaw.split(":");
      String[] values = valueRaw.split(":");

      int a = ids.length;
      int b = values.length;
      int l = a < b ? a : b;

      for (int i = 0; i < l; i++) {
        try {
          values[i] = values[i].replaceAll("%(?![0-9a-fA-F]{2})", "%25");
          if (!URLDecoder.decode(values[i], "UTF-8").equals(keyword) && !URLDecoder.decode(values[i], "UTF-8").equals("")) {
            String item = URLDecoder.decode(values[i], "UTF-8").trim();
            if (item.contains("%2b") || item.contains("%20") || item.contains("%25")) {
              item = item.replace("%2b", " ");
              item = item.replace("%20", " ");
              item = item.replace("%25", " ");
            }
            item = item.replaceAll("[-+^:,*_\"]", " ").replace("\\", " ").replaceAll("\\s+", " ").trim();
            filterValues.put(ids[i], item);
          }
        } catch (Exception e) {
          LOG.error(values[i]);
          e.printStackTrace();
        }
      }
    }

    if (mapRequest.get("temporalsearch") != null) {
      String temporalsearch = mapRequest.get("temporalsearch");
      temporalsearch = URLDecoder.decode(temporalsearch.replaceAll("%(?![0-9a-fA-F]{2})", "%25"), "UTF-8");

      filterValues.put("temporalsearch", temporalsearch);
    }

    return filterValues;
  }

}
