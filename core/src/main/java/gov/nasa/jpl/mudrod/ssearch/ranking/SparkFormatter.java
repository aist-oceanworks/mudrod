package gov.nasa.jpl.mudrod.ssearch.ranking;

import java.io.*;
import java.text.DecimalFormat;

public class SparkFormatter {
  DecimalFormat NDForm = new DecimalFormat("#.###");

  public SparkFormatter() {
  }

  public void toSparkSVMformat(String inputCSVFileName, String outputTXTFileName) {
    File file = new File(outputTXTFileName);
    if (file.exists()) {
      file.delete();
    }
    try {
      file.createNewFile();
      FileWriter fw = new FileWriter(outputTXTFileName);
      BufferedWriter bw = new BufferedWriter(fw);

      BufferedReader br = new BufferedReader(new FileReader(inputCSVFileName));
      br.readLine();
      String line = br.readLine();
      while (line != null) {
        String[] list = line.split(",");
        String output = "";
        Double label = Double.parseDouble(list[list.length - 1].replace("\"", ""));
        if (label == -1.0) {
          output = "0 ";
        } else if (label == 1.0) {
          output = "1 ";
        }

        for (int i = 0; i < list.length - 1; i++) {
          int index = i + 1;
          output += index + ":" + NDForm.format(Double.parseDouble(list[i].replace("\"", ""))) + " ";
        }
        bw.write(output + "\n");

        line = br.readLine();
      }
      br.close();
      bw.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    SparkFormatter sf = new SparkFormatter();
    sf.toSparkSVMformat("C:/mudrodCoreTestData/rankingResults/inputDataForSVM.csv", "C:/mudrodCoreTestData/rankingResults/inputDataForSVM_spark.txt");
  }

}
