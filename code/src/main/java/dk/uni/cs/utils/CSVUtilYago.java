package dk.uni.cs.utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

public class CSVUtilYago {
    static String[] queryTypes = { "c", "f", "s"};
    static HashMap<String, Integer> queryCountInit = new HashMap();
    static HashMap<String, Integer> queryCountEnd = new HashMap();
    
    static String outputFileName = "SA_vs_BL";
    static boolean planningFlag = true;
    
    public static void main(String[] args) {
        String csvFileA = "RESULTS_YAGO/1_DecMinOfMin/1_Dec_YAGO_SA.csv";
        String csvFileB = "RESULTS_YAGO/1_DecMinOfMin/1_Dec_YAGO_BL.csv";
        String outputFile = "RESULTS_YAGO/1_DecMinOfMin/";
        
        String typeA = "Shape";
        String typeB = "BL";
        String cvsSplitBy = ",";
        
        queryCountInit.put("c", 1);
//        queryCountInit.put("m", 1);
        queryCountInit.put("f", 1);
        queryCountInit.put("s", 1);
    
        queryCountEnd.put("c", 3);
//        queryCountEnd.put("m", 5);
        queryCountEnd.put("f", 5);
        queryCountEnd.put("s", 15);
        
        if (planningFlag)
            outputFileName = outputFileName + "-planning";
        
        try {
            outputFile = outputFile + outputFileName;
            outputFile = outputFile + ".csv";
            standardQueryResults(csvFileA, csvFileB, outputFile, typeA, typeB, cvsSplitBy);
        } catch (IOException e) {
            e.printStackTrace();
        }
        
    }
    
    private static void standardQueryResults(String csvFileA, String csvFileB, String outputFile, String typeA, String typeB, String cvsSplitBy) throws IOException {
        
        
        RdfUtils.writeToFileInAppendMode("Query," + "Type" + "," + "Time", outputFile);
        
        for (String query : queryTypes) {
            for (int i = queryCountInit.get(query); i <= queryCountEnd.get(query); i++) {
                String fileFlag = query + i;
                BufferedReader brA = new BufferedReader(new FileReader(csvFileA));
                BufferedReader brB = new BufferedReader(new FileReader(csvFileB));
                runToGetStandardQueriesResults(outputFile, fileFlag, typeB, brB, cvsSplitBy);
                runToGetStandardQueriesResults(outputFile, fileFlag, typeA, brA, cvsSplitBy);
            }
        }
    }
    
    private static void runToGetStandardQueriesResults(String outputFile, String fileFlag, String typeA, BufferedReader br, String cvsSplitBy) throws IOException {
        String line;
        int lineCounter = 0;
        while ((line = br.readLine()) != null) {
            
            // use comma as separator
            String[] row = line.split(cvsSplitBy);
            System.out.println(Arrays.toString(row));
            //System.out.println("Country [code= " + country[4] + " , name=" + country[5] + "]");
            if (lineCounter == 0) {
                //RdfUtils.writeToFileInAppendMode("Query," + "Type" + "," + "Time" ,outputFile);
                for (int i = 1; i <= 10; i++) {
                    System.out.println(row[i] + " + " + row[i + 10]);
                }
            } else {
                //total time = planning + execution time
                
                if (row[0].equals(fileFlag)) {
                    for (int i = 1; i <= 10; i++) {
                        StringBuilder newRow = new StringBuilder(row[0] + ",");
                        double totalTime = Double.parseDouble(row[i]) + Double.parseDouble(row[i + 10]);
                        if (planningFlag) {
                            totalTime = Double.parseDouble(row[i]);
                        }
                        newRow.append(typeA).append(",").append(totalTime);
                        RdfUtils.writeToFileInAppendMode(newRow + "", outputFile);
                    }
                }
            }
            lineCounter++;
        }
    }
    
}


