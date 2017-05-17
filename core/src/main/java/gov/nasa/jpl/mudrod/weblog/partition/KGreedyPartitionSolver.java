package gov.nasa.jpl.mudrod.weblog.partition;

import java.util.*;

public class KGreedyPartitionSolver implements ThePartitionProblemSolver {

  public boolean bsorted = false;

  public KGreedyPartitionSolver() {
    // default constructor
  }

  public KGreedyPartitionSolver(boolean bsorted) {
    this.bsorted = true;
  }

  @Override
  public Map<String, Integer> solve(Map<String, Double> labelNums, int k) {
    List<Double> lista = null;
    List<String> months = null;

    if (!this.bsorted) {
      LinkedHashMap sortedMap = this.sortMapByValue(labelNums);
      lista = new ArrayList(sortedMap.values());
      months = new ArrayList(sortedMap.keySet());
    } else {
      lista = new ArrayList(labelNums.values());
      months = new ArrayList(labelNums.keySet());
    }

    List<List<Double>> parts = new ArrayList<>();
    List<List<String>> splitMonths = new ArrayList<>();

    for (int i = 0; i < k; i++) {
      List<Double> part = new ArrayList();
      parts.add(part);

      List<String> monthList = new ArrayList();
      splitMonths.add(monthList);
    }

    int j = 0;
    for (Double lista1 : lista) {

      Double minimalSum = -1.0;
      int position = 0;
      for (int i = 0; i < parts.size(); i++) {
        List<Double> part = parts.get(i);
        if (minimalSum == -1) {
          minimalSum = Suma(part);
          position = i;
        } else if (Suma(part) < minimalSum) {
          minimalSum = Suma(part);
          position = i;
        }
      }

      List<Double> part = parts.get(position);
      part.add(lista1);
      parts.set(position, part);

      List<String> month = splitMonths.get(position);
      month.add(months.get(j));
      splitMonths.set(position, month);
      j++;
    }

    /*  for(int i=0; i<splitMonths.size(); i++){
        System.out.println("group:" + i);
        printStrList(splitMonths.get(i));
      }
      
      for(int i=0; i<parts.size(); i++){
        print(parts.get(i));
      }*/

    Map<String, Integer> LabelGroups = new HashMap<String, Integer>();
    for (int i = 0; i < splitMonths.size(); i++) {
      List<String> list = splitMonths.get(i);
      for (int m = 0; m < list.size(); m++) {
        LabelGroups.put(list.get(m), i);
      }
    }

    return LabelGroups;
  }

  public LinkedHashMap<String, Double> sortMapByValue(Map passedMap) {
    List mapKeys = new ArrayList(passedMap.keySet());
    List mapValues = new ArrayList(passedMap.values());
    Collections.sort(mapValues, Collections.reverseOrder());
    Collections.sort(mapKeys, Collections.reverseOrder());

    LinkedHashMap sortedMap = new LinkedHashMap();

    Iterator valueIt = mapValues.iterator();
    while (valueIt.hasNext()) {
      Object val = valueIt.next();
      Iterator keyIt = mapKeys.iterator();

      while (keyIt.hasNext()) {
        Object key = keyIt.next();
        String comp1 = passedMap.get(key).toString();
        String comp2 = val.toString();

        if (comp1.equals(comp2)) {
          passedMap.remove(key);
          mapKeys.remove(key);
          sortedMap.put((String) key, (Double) val);
          break;
        }

      }

    }
    return sortedMap;
  }

  private Double Suma(List<Double> part) {
    Double ret = 0.0;
    for (int i = 0; i < part.size(); i++) {
      ret += part.get(i);
    }
    return ret;
  }

  private void print(List<Double> list) {
    /*for (int i = 0; i < list.size(); i++) {
        System.out.print(list.get(i)+",");
    }*/
    System.out.print("sum is:" + Suma(list));
    System.out.println();
  }

  private void printStrList(List<String> list) {
    for (int i = 0; i < list.size(); i++) {
      System.out.print(list.get(i) + ",");
    }
    System.out.println();
  }

}
