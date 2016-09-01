package esiptestbed.mudrod.weblog.structure;

import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import esiptestbed.mudrod.weblog.pre.CrawlerDetection;

/**
 * This class represents an Apache access log line. See
 * http://httpd.apache.org/docs/2.2/logs.html for more details.
 */
public class ApacheAccessLog extends WebLog implements Serializable {

  double Bytes;
  String Referer;
  String Browser;

  @Override
  public double getBytes() {
    return this.Bytes;
  }

  @Override
  public String getBrowser() {
    return this.Browser;
  }

  public ApacheAccessLog() {

  }

  public static ApacheAccessLog parseFromLogLine(String log)
      throws IOException, ParseException {

    String logEntryPattern = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+|-) \"((?:[^\"]|\")+)\" \"([^\"]+)\"";
    final int NUM_FIELDS = 9;
    Pattern p = Pattern.compile(logEntryPattern);
    Matcher matcher;

    String lineJson = "{}";
    matcher = p.matcher(log);
    if (!matcher.matches() || NUM_FIELDS != matcher.groupCount()) {
      return null;
    }

    String time = matcher.group(4);
    time = SwithtoNum(time);
    SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
    Date date = formatter.parse(time);

    String bytes = matcher.group(7);

    if (bytes.equals("-")) {
      bytes = "0";
    }

    String request = matcher.group(5).toLowerCase();
    String agent = matcher.group(9);
    CrawlerDetection crawlerDe = new CrawlerDetection();
    if (crawlerDe.checkKnownCrawler(agent)) {
      return null;
    } else {

      boolean tag = false;
      String[] mimeTypes = { ".js", ".css", ".jpg", ".png", ".ico",
          "image_captcha", "autocomplete", ".gif", "/alldata/", "/api/",
          "get / http/1.1", ".jpeg", "/ws/" };
      for (int i = 0; i < mimeTypes.length; i++) {
        if (request.contains(mimeTypes[i])) {
          tag = true;
          return null;
        }
      }

      if (tag == false) {
        ApacheAccessLog accesslog = new ApacheAccessLog();
        accesslog.LogType = "PO.DAAC";
        accesslog.IP = matcher.group(1);
        accesslog.Time = date;
        accesslog.Request = matcher.group(5);
        accesslog.Response = matcher.group(6);
        accesslog.Bytes = Double.parseDouble(bytes);
        accesslog.Referer = matcher.group(8);
        accesslog.Browser = matcher.group(9);
        return accesslog;
      }
    }

    return null;
  }
}
