package esiptestbed.mudrod.ssearch.ranking;

import java.util.ArrayList;
import java.util.Map;
import java.util.regex.Pattern;

public class AttributeExtractor {

  public AttributeExtractor() {
  }
  
  public Double getVersionNum(String version)
  {
    if(version==null){
      return 0.0;
    }
    Double versionNum = 0.0;
    Pattern p = Pattern.compile(".*[a-zA-Z].*");
    if(version.equals("Operational/Near-Real-Time"))
    {
      versionNum = 2.0;
    }else if(version.matches("[0-9]{1}[a-zA-Z]{1}"))
    {
      versionNum = Double.parseDouble(version.substring(0, 1));
    }else if(p.matcher(version).find())
    {
      versionNum = 0.0;
    }else
    {
      versionNum = Double.parseDouble(version);
      if(versionNum >=5)
      {
        versionNum = 20.0;
      }
    }
    return versionNum;
  }

  /**
   * Method of converting processing level string into a number
   *
   * @param pro
   *          processing level string
   * @return processing level number
   */
  public Double getProLevelNum(String pro) {
    if (pro == null) {
      return 1.0;
    }
    Double proNum;
    Pattern p = Pattern.compile(".*[a-zA-Z].*");
    if (pro.matches("[0-9]{1}[a-zA-Z]{1}")) {
      proNum = Double.parseDouble(pro.substring(0, 1));
    } else if (p.matcher(pro).find()) {
      proNum = 1.0;
    } else {
      proNum = Double.parseDouble(pro);
    }

    return proNum;
  }

  public Double getSpatialR(Map<String, Object> result)
  {
    Double spatialR;
    if(result.get("Dataset-SatelliteSpatialResolution") != null)
    {
      spatialR = (Double) result.get("Dataset-SatelliteSpatialResolution");
    }else
    {
      Double gridR = (Double) result.get("Dataset-GridSpatialResolution");
      if(gridR != null)
      {
        spatialR = 111 * gridR;
      }else{
        spatialR = 25.0;
      }
    }

    return spatialR;
  }

  public Double getTemporalR(Map<String, Object> result)
  {
    String tr_str = (String) result.get("Dataset-TemporalResolution");
    if(tr_str.equals(""))
    {
      tr_str = (String) result.get("Dataset-TemporalRepeat");
    }

    return covertTimeUnit(tr_str);
  }

  public Double covertTimeUnit(String str)
  {
    Double timeInHour;
    if(str.contains("Hour"))
    {
      timeInHour = Double.parseDouble(str.split(" ")[0]);
    }else if(str.contains("Day"))
    {
      timeInHour = Double.parseDouble(str.split(" ")[0]) * 24;
    }else if(str.contains("Week"))
    {
      timeInHour = Double.parseDouble(str.split(" ")[0]) * 24 * 7;
    }else if(str.contains("Month"))
    {
      timeInHour = Double.parseDouble(str.split(" ")[0]) * 24 * 7 * 30;
    }else if(str.contains("Year"))
    {
      timeInHour = Double.parseDouble(str.split(" ")[0]) * 24 * 7 * 30 * 365;
    }else
    {
      timeInHour = 0.0;
    }

    return timeInHour;
  }

  public Double getPop(Double pop) {
    if (pop > 1000) {
      pop = 1000.0;
    }
    return pop;
  }

  /**
   * Method of checking if query exists in a certain attribute
   *
   * @param strList
   *          attribute value in the form of ArrayList
   * @param query
   *          query string
   * @return 1 means query exists, 0 otherwise
   */
  public Double exists(ArrayList<String> strList, String query) {
    Double val = 0.0;
    if (strList != null) {
      String str = String.join(", ", strList);
      if (str != null && str.length() != 0 && str.toLowerCase().trim().contains(query)) {
        val = 1.0;
      }
    }
    return val;
  }

}
