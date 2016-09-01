package esiptestbed.mudrod.weblog.structure;

import java.io.Serializable;
import java.util.Date;

/**
 * This class represents an Apache access log line. See
 * http://httpd.apache.org/docs/2.2/logs.html for more details.
 */
public class WebLog implements Serializable {
  String LogType;
  String IP;
  Date Time;
  String Request;
  String Response;
  double Bytes;
  String Referer;
  String Browser;

  public String getLogType() {
    return this.LogType;
  }

  public String getIP() {
    return this.IP;
  }

  public Date getTime() {
    return this.Time;
  }

  public String getRequest() {
    return this.Request;
  }

  public String getResponse() {
    return this.Response;
  }

  public double getBytes() {
    return this.Bytes;
  }

  public String getBrowser() {
    return this.Browser;
  }

  public WebLog() {

  }

  public static String SwithtoNum(String time) {
    if (time.contains("Jan")) {
      time = time.replace("Jan", "1");
    } else if (time.contains("Feb")) {
      time = time.replace("Feb", "2");
    } else if (time.contains("Mar")) {
      time = time.replace("Mar", "3");
    } else if (time.contains("Apr")) {
      time = time.replace("Apr", "4");
    } else if (time.contains("May")) {
      time = time.replace("May", "5");
    } else if (time.contains("Jun")) {
      time = time.replace("Jun", "6");
    } else if (time.contains("Jul")) {
      time = time.replace("Jul", "7");
    } else if (time.contains("Aug")) {
      time = time.replace("Aug", "8");
    } else if (time.contains("Sep")) {
      time = time.replace("Sep", "9");
    } else if (time.contains("Oct")) {
      time = time.replace("Oct", "10");
    } else if (time.contains("Nov")) {
      time = time.replace("Nov", "11");
    } else if (time.contains("Dec")) {
      time = time.replace("Dec", "12");
    }
    return time;
  }

  public static boolean checknull(WebLog s) {
    if (s == null) {
      return false;
    }
    return true;
  }
}
