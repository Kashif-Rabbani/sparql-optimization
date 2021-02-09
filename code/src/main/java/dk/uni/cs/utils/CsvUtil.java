package dk.uni.cs.utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

public class CsvUtil {

    public static void main(String[] args) {
//        String csvFileA = "RESULTS_YAGO/ResultsWithNewQuerySet/DualApproach/yagoJenaDualApproachBaseQueries6Oct.csv";
//        String csvFileB = "RESULTS_YAGO/ResultsWithNewQuerySet/DualApproach/NewCostModel/yagoShapeDualApproachAllQueries10Oct.csv";
//        "RESULTS_YAGO/ResultsWithNewQuerySet/YagoShape113QueriesShuffled20Sept.csv";

//        String csvFileA = "RESULTS_WATDIV/ResultsNewQuerySet/DualApproach/watDivJenaDualApproachBaseQueries6Oct.csv";
//        String csvFileB = "RESULTS_WATDIV/ResultsNewQuerySet/DualApproach/NewCostModel/watDivShapeDualApproachAllQueries10Oct.csv";
        String csvFileA = "RESULTS_LUBM/ResultsNewQuerySet/DualApproach/lubmJenaDualApproachBaseQueries6Oct.csv";
        String csvFileB = "RESULTS_LUBM/ResultsNewQuerySet/DualApproach/NewCostModel/lubmShapeDualApproachBaseQueries10Oct.csv";
//        "RESULTS_LUBM/ResultsNewQuerySet/lubmShape22Queries24Sept.csv";
    
//        String csvFileA = "RESULTS_YAGO/ResultsWithNewQuerySet/DualApproach/Templates/MANUAL_JENA.csv";
//        String csvFileB = "RESULTS_YAGO/ResultsWithNewQuerySet/DualApproach/Templates/MANUAL_SHAPE.csv";
        String outputFile = "RESULTS_LUBM/ResultsNewQuerySet/DualApproach/NewCostModel/";
        
        String typeA = "Jena";
        String typeB = "Shape";
        String cvsSplitBy = ",";
        
        try {
            standardQueryResults(csvFileA, csvFileB, outputFile, typeA, typeB, cvsSplitBy);
            //templateQueryResults(csvFileA, csvFileB, outputFile, typeA, typeB, cvsSplitBy);
        } catch (IOException e) {
            e.printStackTrace();
        }
        
    }
    
    private static void templateQueryResults(String csvFileA, String csvFileB, String outputFile, String typeA, String typeB, String cvsSplitBy) throws IOException {
        for (int i = 1; i <= 7; i++) {
            String fileFlag = "S" + i;
            RdfUtils.writeToFileInAppendMode("Query," + "Type" + "," + "Time", outputFile + fileFlag + ".csv");
            BufferedReader brA = new BufferedReader(new FileReader(csvFileA));
            BufferedReader brB = new BufferedReader(new FileReader(csvFileB));
            
            
            runToGetTemplateQueriesResults(outputFile, fileFlag, typeB, brB, cvsSplitBy);
            runToGetTemplateQueriesResults(outputFile, fileFlag, typeA, brA, cvsSplitBy);
        }
        
    }
    
    private static void standardQueryResults(String csvFileA, String csvFileB, String outputFile, String typeA, String typeB, String cvsSplitBy) throws IOException {
        outputFile = outputFile + "lubmDualApproachBaseQueries10-Oct-planning.csv";
        RdfUtils.writeToFileInAppendMode("Query," + "Type" + "," + "Time", outputFile);
        for (int i = 1; i <= 2; i++) {
            String fileFlag = "s" + i;
            BufferedReader brA = new BufferedReader(new FileReader(csvFileA));
            BufferedReader brB = new BufferedReader(new FileReader(csvFileB));
            runToGetStandardQueriesResults(outputFile, fileFlag, typeB, brB, cvsSplitBy);
            runToGetStandardQueriesResults(outputFile, fileFlag, typeA, brA, cvsSplitBy);
        }
    }
    
    
    private static void runToGetTemplateQueriesResults(String outputFile, String fileFlag, String typeA, BufferedReader br, String cvsSplitBy) throws IOException {
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
                
                if (row[0].contains(fileFlag) && !row[0].equals(fileFlag)) {
                    for (int i = 1; i <= 10; i++) {
                        StringBuilder newRow = new StringBuilder(row[0] + ",");
                        //long totalTime = Long.parseLong(row[i]) + Long.parseLong(row[i + 10]);
                        long totalTime = Long.parseLong(row[i]);
                        newRow.append(typeA).append(",").append(totalTime);
                        RdfUtils.writeToFileInAppendMode(newRow + "", outputFile + fileFlag + ".csv");
                    }
                }
            }
            lineCounter++;
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
                        //double totalTime = Double.parseDouble(row[i]) + Double.parseDouble(row[i + 10]);
                        double totalTime = Double.parseDouble(row[i]);
                        newRow.append(typeA).append(",").append(totalTime);
                        RdfUtils.writeToFileInAppendMode(newRow + "", outputFile);
                    }
                }
            }
            lineCounter++;
        }
    }
    
}


