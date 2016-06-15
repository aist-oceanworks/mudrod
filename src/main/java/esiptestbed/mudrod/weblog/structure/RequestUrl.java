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

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse.AnalyzeToken;

import esiptestbed.mudrod.discoveryengine.MudrodAbstract;
import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;

public class RequestUrl extends MudrodAbstract {
  public RequestUrl(Map<String, String> config, ESDriver es,
      SparkDriver spark) {
    super(config, es, spark);
    // TODO Auto-generated constructor stub
  }

  public static String UrlPage(String strURL) {
    String strPage = null;
    String[] arrSplit = null;

    strURL = strURL.trim().toLowerCase();

    arrSplit = strURL.split("[?]");
    if (strURL.length() > 0) {
      if (arrSplit.length > 1) {
        if (arrSplit[0] != null) {
          strPage = arrSplit[0];
        }
      }
    }

    return strPage;
  }

  private static String TruncateUrlPage(String strURL) {
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

  public static Map<String, String> URLRequest(String URL) {
    Map<String, String> mapRequest = new HashMap<String, String>();

    String[] arrSplit = null;

    String strUrlParam = TruncateUrlPage(URL);
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

  public String GetSearchInfo(String URL) throws UnsupportedEncodingException {
    // String info = "";
    List<String> info = new ArrayList<String>();
    String keyword = "";
    Map<String, String> mapRequest = RequestUrl.URLRequest(URL);
    if (mapRequest.get("search") != null) {
      try {
        keyword = mapRequest.get("search");

        keyword = URLDecoder
            .decode(keyword.replaceAll("%(?![0-9a-fA-F]{2})", "%25"), "UTF-8");
        if (keyword.contains("%2b") || keyword.contains("%20")
            || keyword.contains("%25")) {
          keyword = keyword.replace("%2b", " ");
          keyword = keyword.replace("%20", " ");
          keyword = keyword.replace("%25", " ");
        }

        // keyword = keyword.replaceAll("[-+.^:,*_]"," ").replaceAll("\\s+","
        // ");
        keyword = keyword.replaceAll("[-+^:,*_\"]", " ").replace("\\", " ")
            .replaceAll("\\s+", " ").trim();

      } catch (UnsupportedEncodingException e) {
        // TODO Auto-generated catch block
        System.out.println(mapRequest.get("search"));
        e.printStackTrace();
      }
      // keyword = keyword.replace("\"", "").toLowerCase().trim();
      if (!keyword.equals("")) {
        // info = keyword + ",";
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
        if (ids[i].equals("collections") || ids[i].equals("measurement")
            || ids[i].equals("sensor") || ids[i].equals("platform")
            || ids[i].equals("variable")) {
          try {
            values[i] = values[i].replaceAll("%(?![0-9a-fA-F]{2})", "%25");
            if (!URLDecoder.decode(values[i], "UTF-8").equals(keyword)
                && !URLDecoder.decode(values[i], "UTF-8").equals("")) {
              // info = info + URLDecoder.decode(values[i], "UTF-8").trim() +
              // ",";
              String item = URLDecoder.decode(values[i], "UTF-8").trim();
              if (item.contains("%2b") || item.contains("%20")
                  || item.contains("%25")) {
                item = item.replace("%2b", " ");
                item = item.replace("%20", " ");
                item = item.replace("%25", " ");
              }
              // item = item.replaceAll("[-+.^:,*_]"," ").replaceAll("\\s+","
              // ").trim();
              item = item.replaceAll("[-+^:,*_\"]", " ").replace("\\", " ")
                  .replaceAll("\\s+", " ").trim();
              info.add(item);
            }
          } catch (Exception e) {
            // TODO Auto-generated catch block
            System.out.println(values[i]);
            e.printStackTrace();
          }
        }

      }
    }

    String info_str = String.join(",", info);

    try {
      return es.customAnalyzing(config.get("indexName"), info_str);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (ExecutionException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    return null;
  }

  public static String GetSearchWord(String URL)
      throws UnsupportedEncodingException {
    String keyword = "";

    Map<String, String> mapRequest = RequestUrl.URLRequest(URL);
    if (mapRequest.get("search") != null) {
      try {
        keyword = mapRequest.get("search");

        keyword = URLDecoder
            .decode(keyword.replaceAll("%(?![0-9a-fA-F]{2})", "%25"), "UTF-8");
        if (keyword.contains("%2b") || keyword.contains("%20")
            || keyword.contains("%25")) {
          keyword = keyword.replace("%2b", " ");
          keyword = keyword.replace("%20", " ");
          keyword = keyword.replace("%25", " ");
        }

        // keyword = keyword.replaceAll("[-+.^:,*_]"," ").replaceAll("\\s+","
        // ");
        keyword = keyword.replaceAll("[-+^:,*_\"]", " ").replace("\\", " ")
            .replaceAll("\\s+", " ").trim();
      } catch (UnsupportedEncodingException e) {
        // TODO Auto-generated catch block
        System.out.println(mapRequest.get("search"));
        e.printStackTrace();
      }
    }

    return keyword;
  }

  public static Map<String, String> GetFilterInfo(String URL)
      throws UnsupportedEncodingException {
    List<String> info = new ArrayList<String>();
    Map<String, String> filterValues = new HashMap<String, String>();

    String keyword = "";
    Map<String, String> mapRequest = RequestUrl.URLRequest(URL);
    if (mapRequest.get("search") != null) {
      try {
        keyword = mapRequest.get("search");

        keyword = URLDecoder
            .decode(keyword.replaceAll("%(?![0-9a-fA-F]{2})", "%25"), "UTF-8");
        if (keyword.contains("%2b") || keyword.contains("%20")
            || keyword.contains("%25")) {
          keyword = keyword.replace("%2b", " ");
          keyword = keyword.replace("%20", " ");
          keyword = keyword.replace("%25", " ");
        }

        // keyword = keyword.replaceAll("[-+.^:,*_]"," ").replaceAll("\\s+","
        // ");
        keyword = keyword.replaceAll("[-+^:,*_\"]", " ").replace("\\", " ")
            .replaceAll("\\s+", " ").trim();

      } catch (UnsupportedEncodingException e) {
        // TODO Auto-generated catch block
        System.out.println(mapRequest.get("search"));
        e.printStackTrace();
      }
      // keyword = keyword.replace("\"", "").toLowerCase().trim();
      if (!keyword.equals("")) {
        // info = keyword + ",";
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
        try {
          values[i] = values[i].replaceAll("%(?![0-9a-fA-F]{2})", "%25");
          if (!URLDecoder.decode(values[i], "UTF-8").equals(keyword)
              && !URLDecoder.decode(values[i], "UTF-8").equals("")) {
            String item = URLDecoder.decode(values[i], "UTF-8").trim();
            if (item.contains("%2b") || item.contains("%20")
                || item.contains("%25")) {
              item = item.replace("%2b", " ");
              item = item.replace("%20", " ");
              item = item.replace("%25", " ");
            }
            item = item.replaceAll("[-+^:,*_\"]", " ").replace("\\", " ")
                .replaceAll("\\s+", " ").trim();
            filterValues.put(ids[i], item);
          }
        } catch (Exception e) {
          // TODO Auto-generated catch block
          System.out.println(values[i]);
          e.printStackTrace();
        }
      }
    }

    if (mapRequest.get("temporalsearch") != null) {
      String temporalsearch = mapRequest.get("temporalsearch");
      temporalsearch = URLDecoder.decode(
          temporalsearch.replaceAll("%(?![0-9a-fA-F]{2})", "%25"), "UTF-8");

      filterValues.put("temporalsearch", temporalsearch);
    }

    return filterValues;
  }

}
