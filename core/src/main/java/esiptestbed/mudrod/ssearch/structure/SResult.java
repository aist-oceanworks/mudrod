package esiptestbed.mudrod.ssearch.structure;

import java.lang.reflect.Field;

public class SResult {
//may replace it with Map<String,Object>
  String shortName = null;
  String longName = null;
  String topic = null;
  String description = null;
  String date = null;

  public long dateLong = 0;
  public Double clicks = null;
  public Double relevance = null;

  public Double final_score = 0.0;
  public Double term_score = 0.0;
  public Double releaseDate_score = 0.0;
  public Double click_score = 0.0;


  public SResult(String shortName, String longName, String topic, String description, String date){
    this.shortName = shortName;
    this.longName = longName;
    this.topic = topic;
    this.description = description;
    this.date = date;
  }

/*  public void setDateLong(long dateL)
  {
    dateLong = dateL;
  }
  
  public void setClicks(Double s)
  {
    clicks = s;
  }

  public void setRelevance(Double s)
  {
    relevance = s;
  }


  public void setFinalScore(Double s)
  {
    final_score = s;
  }

  public void setTermScore(Double s)
  {
    term_score = s;
  }

  public void setReleaseScore(Double s)
  {
    releaseDate_score = s;
  }

  public void setClickScore(Double s)
  {
    click_score = s;
  }*/

  public static boolean set(Object object, String fieldName, Object fieldValue) {
    Class<?> clazz = object.getClass();
    while (clazz != null) {
      try {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(object, fieldValue);
        return true;
      } catch (NoSuchFieldException e) {
        clazz = clazz.getSuperclass();
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  public static <V> V get(Object object, String fieldName) {
    Class<?> clazz = object.getClass();
    while (clazz != null) {
      try {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        return (V) field.get(object);
      } catch (NoSuchFieldException e) {
        clazz = clazz.getSuperclass();
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }
    return null;
  }

}
