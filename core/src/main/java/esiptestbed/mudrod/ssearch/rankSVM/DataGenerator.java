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
package esiptestbed.mudrod.ssearch.rankSVM;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;

/*import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;*/

/**
 * SVMData is a program designed to create appropriate input data for the RankSVM
 * algorithm that involves Pairwise Classification.  Specifically, instead of working in
 * the space of query-document vectors, e.g. x1, x2, x3, we transform them into a new space 
 * in which a pair of documents is represented as the difference between their feature vectors.
 *
 * @author Joseph J. George, University of Richmond
 */
public class DataGenerator {

  private static String mySourceDir;
  private static String myResultDir;
  private static boolean isMultFiles;
  
  private static String[] myHeader;
  private static List<List<String>> myMasterList = new ArrayList<List<String>>();
  
  // HashMap used for comparing evaluation classes
  public static final HashMap<String, Integer> map1 = new HashMap<String, Integer>();
  static
  {
    map1.put("Excellent",5);
    map1.put("Good",4);
    map1.put("OK",3);
    map1.put("Bad",2);
    map1.put("Terrible",1);
  }
  
  /**
   * Constructor which takes in path containing one or multiple files to process.  
   * Also takes in argument specifying whether or not a single file needs to be processed,
   * or multiple files need to be processed.
   * 
   * @param fileDir directory containing single file or multiple files to be processed
   * @param multFiles true if multiple files in directory need to be processed and false if
   *                  only a single file needs to be processed
   */
  public DataGenerator(String sourceDir, String resultDir, boolean multFiles)
  {
    mySourceDir = sourceDir;
    myResultDir = resultDir;
    isMultFiles = multFiles;
  }
  
  
  /**
   * Responsible for invoking the processing of data file(s) and their subsequent storage 
   * into a user specified directory.
   */
  public void process()
  {
    parseFile();
    writeCSVfile(myMasterList);
  }
  
  /**
   * Parses the original user-specified CSV file, storing the contents for future calculations
   * and formatting.
   */
  public static void parseFile()
  {
    String[][] dataArr = null;
    try
    {
      String sourceDir = mySourceDir;
      
      if(isMultFiles == true) // Case where multiple files have to be processed
      {
        // Iterate over files in directory 
        File directory = new File(sourceDir);
        File[] directoryListing = directory.listFiles();
        
        if(directoryListing != null)
        {
          for(File child : directoryListing)
          {
            CSVReader csvReader = new CSVReader(new FileReader(child));
            List<String[]> list = csvReader.readAll();
            
            // Store into 2D array by transforming array list to normal array
            dataArr = new String[list.size()][];
            dataArr = list.toArray(dataArr);

            calculateVec(dataArr);
            
            csvReader.close();
          }
          storeHead(dataArr); // Store the header
        }
      }
      else // Process only one file
      {
        File file = new File(sourceDir);
        
        if(file != null)
        {
          CSVReader csvReader = new CSVReader(new FileReader(file));
          List<String[]> list = csvReader.readAll();
          
          // Store into 2D array by transforming array list to normal array
          dataArr = new String[list.size()][];
          dataArr = list.toArray(dataArr);

          storeHead(dataArr); // Store the header
          calculateVec(dataArr);
          
          csvReader.close();
        }
      }
    }
    catch(FileNotFoundException e)
    {
      e.printStackTrace();
    }
    catch(IOException e)
    {
      e.printStackTrace();
    } 
  }
  
  /**
   * Performs the necessary vector calculations on each possible combination of vectors,
   * also storing a value that indicates the evaluation.
   * 
   * @param arr the parsed contents of the original CSV file
   */
  public static void calculateVec(String[][] arr)
  {
    List<List<String>> listofLists = new ArrayList<List<String>>(); // Holds calculations 
    
    int rowStart = 1;
    while(rowStart < arr.length) // Perform calculations for each row continuously until last is reached
    {
      for(int row = rowStart; row < arr.length - 1; row++) // Start at row 1 because row 0 is heading lol
      {
        List<String> colList = new ArrayList<String>(); // create vector to store all values inside of a column, which is stored inside 2D vector
        for(int col = 0; col < arr[0].length - 1; col++) // Columns go until the next to last column 
        {
          
          // Extract double value from each cell
          double x1 = Double.parseDouble(arr[row][col]);
          double x2 = Double.parseDouble(arr[row + 1][col]);
          
          // Perform calculation for each cell
          double result = x1-x2;
          
          // Convert this double value into string, and store inside array list
          String strResult = Double.toString(result);
          colList.add(strResult);
        }
        
        // Finally, add either 1, -1, or do not add row at all when encountering evaluation value
        int addEvalNum = compareEvaluation(arr[row][arr[0].length - 1], arr[row + 1][arr[0].length - 1]);
        if(addEvalNum == 1) 
        {
          colList.add("1");
          listofLists.add(colList); // Add this list to 2D list - row is finished now, move on
        }
        else if(addEvalNum == -1)
        {
          colList.add("-1");
          listofLists.add(colList); // Add this list to 2D list - row is finished now, move on
        }
        // Else, they are equal, do not even add this row to 2D vector
      }
      
      rowStart++;  // Start calculations all over, this time beginning at the next row
    }
    
    // After all processing takes place, send to method that recreates data with equal # of 1's and -1's
    List<List<String>> equalizedList = equalizeList(listofLists);
    myMasterList.addAll(equalizedList);
  } 
  
  /**
   * Taking in two vector evaluation parameters, compares these two evaluations, returning a 1
   * if the first evaluation is greater than the second, a -1 in the case the first evaluation is
   * less than the second, and a 10 in the case that the two are equal, meaning this vector will
   * not be used.
   * 
   * @param eval1 evaluation from first vector
   * @param eval2 evaluation from second vector
   * @return 1 if first evaluation is greater than the second, -1 if first evaluation is less than the second, and
   *         10 in the case that the two are equal
   */
  public static int compareEvaluation(String eval1, String eval2)
  {
    int evalNum1 = map1.get(eval1);
    int evalNum2 = map1.get(eval2);
    
    if(evalNum1 > evalNum2) // ">" means it is more relevant - assign a 1
    {
      return 1;
    }
    else if(evalNum1 < evalNum2)
    {
      return -1;
    }
    else
    {
      return 10; // Return 10 if they are equal - signifies you cannot use the row
    }
  }
  
  /**
   * After vector calculations and new evaluation values are set, produces refined output data such that 
   * there is an equal or close to equal number of rows containing both "1" and "-1" as the new evaluation value.
   * 
   * @param rawList originally calculated data from the input CSV file
   * @return data that has an equal distribution of evaluation values
   */
  public static List<List<String>> equalizeList(List<List<String>> rawList)
  {
    // Create two sets - one containing row index for +1 and the other for -1
    List<Integer> pos1List = new ArrayList<Integer>();  
    List<Integer> neg1List = new ArrayList<Integer>(); 
    
    for(int i = 0; i < rawList.size() - 1; i++) // Iterate through all rows to get indexes 
    {
      int evalNum = Integer.parseInt(rawList.get(i).get(rawList.get(0).size() - 1)); // Get 1 or -1 from original array list
      if(evalNum == 1)
      {
        pos1List.add(i); // Add row index that has 1
      }
      else if(evalNum == -1)
      {
        neg1List.add(i); // Add row index that has -1
      }
    }
    
    int count = 0;
    List<List<String>> equalizedList = new ArrayList<List<String>>(); // Holds calculations 
    while(count < pos1List.size() && count < neg1List.size()) // Equally select amongst the two to include in new 2D arr list
    {
      equalizedList.add(rawList.get(pos1List.get(count)));
      equalizedList.add(rawList.get(neg1List.get(count)));
      count++;
    }
    
    return equalizedList;
  }
  
  /**
   * Retrieves the heading from a file to be processed so it can be written to the output file later.
   * 
   * @param arr 2D array containing the parsed information from input file
   */
  public static void storeHead(String[][] arr)
  { 
    myHeader = new String[arr[0].length]; // Reside private variable
    
    for(int col = 0; col < arr[0].length; col++)
    {
      myHeader[col] = arr[0][col];
    }
  }
  
  /**
   * Writes newly calculated and equally distributed vector data to user specified CSV file.
   * 
   * @param list finalized vector data to write to user specified output file
   */
  public static void writeCSVfile(List<List<String>> list)
  {
    String outputFile = myResultDir;
    boolean alreadyExists = new File(outputFile).exists();
    
    try
    {
      CSVWriter csvOutput = new CSVWriter(new FileWriter(outputFile), ','); // Create new instance of CSVWriter to write to file output
      
      if(!alreadyExists)
      {
        csvOutput.writeNext(myHeader); // Write the text headers first before data
        
        for(int i = 0; i < list.size(); i++) // Iterate through all rows in 2D array
        {
          String[] temp = new String[list.get(i).size()]; // Convert row array list in 2D array to regular string array
          temp = list.get(i).toArray(temp);
          csvOutput.writeNext(temp); // Write this array to the file
        }
      }
      
      csvOutput.close(); // Close csvWriter
    }
    catch(IOException e)
    {
      e.printStackTrace();
    }
  }
  
  public static void main(String[] args) {
    DataGenerator dg = new DataGenerator("C:/mudrodCoreTestData/rankingResults/joe.csv", "C:/mudrodCoreTestData/rankingResults/joeTest.csv", false);
    dg.process();
  }

}
