package dk.uni.cs;

import dk.uni.cs.query.pipeline.*;
import dk.uni.cs.shapes.*;
import dk.uni.cs.utils.ConfigManager;
import dk.uni.cs.utils.RdfUtils;
import dk.uni.cs.utils.Tuple3;
import dk.uni.cs.utils.Tuple5;
import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

public class Main {
    final static Logger logger = Logger.getLogger(Main.class);
    public static HashMap<String, List<Tuple3<String, String, String>>> predicateShapeDistMap;
    public static HashMap<String, List<Integer>> shapePropStatsMap;
    public static HashMap<String, List<Statistics>> predicatesToShapesStatsMapping;
    public static HashMap<String, Tuple3<String, String, String>> rdfTypePredicateTargetedNodeShapesMap;
    public static HashMap<String, Statistics> rdfTypePredicateTargetedNodeShapesStatsMap;
    public static String configPath;
    public static String dataset;
    public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");
    public static int allTriplesCount = 0, distinctObjectsCount = 0, distinctSubjectsCount = 0, distinctRdfTypeCount = 0, distinctRdfTypeSubjCount = 0, distinctRdfTypeObjectsCount = 0;
    
    
    public static void main(String[] args) throws Exception {
        configPath = args[0];
        dataset = args[1];
        logger.info("Main started....");
        setGlobalStatistics();
        loadRdfFromMultipleFiles();
        loadRDF();
        loadSHACL();
        reloadSHACL();
        annotateShapesWithStats();
        benchmark();
        writeToFile();
    }
    
    private static void benchmark() {
        if (Boolean.parseBoolean(ConfigManager.getProperty("benchmarkQuery"))) {
            if (Boolean.parseBoolean(ConfigManager.getProperty("shapeExec")) || Boolean.parseBoolean(ConfigManager.getProperty("globalStatsExec")) || Boolean.parseBoolean(ConfigManager.getProperty("executeQueryForOtherPurpose"))) {
                //load the shapes stats in the memory
                RdfUtils.loadShapesIntoMemStore(getShapesModelIRI());
                predicateShapeDistMap = new InMemComputation().getPredicatesCandidateShapesProp();
                shapePropStatsMap = new InMemComputation().getPropertyShapeStatsFromShapeGraph();
                predicatesToShapesStatsMapping = new InMemComputation().getPredicatesCandidateShapesPropStatistics(predicateShapeDistMap);
                rdfTypePredicateTargetedNodeShapesMap = new InMemComputation().getRdfTypePredicatesCandidateShapes();
                rdfTypePredicateTargetedNodeShapesStatsMap = new InMemComputation().getRdfTypePredicatesCandidateShapesStats();
            }
            Starter starter = new Starter();
            System.out.println("Benchmark Completed...");
            System.out.println(starter.getLogTuple());
        }
    }
    
    private static void writeToFile() {
        if (Objects.equals(ConfigManager.getProperty("writeShapeGraphToFile"), "true")) {
            RdfUtils.writeGraphModelToFile(getShapesModelIRI(), ConfigManager.getProperty("shapesGraphFileAddress"));
        }
    }
    
    private static void annotateShapesWithStats() {
        if (Objects.equals(ConfigManager.getProperty("generateStatistics"), "true")) {
            System.out.println("Before: Size of the shape graph " + getShapesModelIRI() + " = " + RdfUtils.getSizeOfModel(getShapesModelIRI()));
            System.out.println("Invoked: new StatsGeneratorGraphDB()");
            new StatsGeneratorGraphDB();
            //generateShapesStatistics();
            //System.out.println("Invoked: addMetadataShape();");
            //addMetadataShape();
            System.out.println("After: Size of the shape graph " + getShapesModelIRI() + " = " + RdfUtils.getSizeOfModel(getShapesModelIRI()));
        } else {
            System.out.println("Shapes graph is already annotated with the statistics");
        }
    }
    
    private static void addMetadataShape() {
        new MetadataShapes().annotate();
    }
    
    private static void reloadSHACL() {
        if (Objects.equals(ConfigManager.getProperty("reloadShapedData"), "true")) {
            
            if (RdfUtils.removeNamedGraph(getShapesModelIRI())) {
                System.out.println("Named graph " + getShapesModelIRI() + " removed successfully.\n Let's load new one");
                RdfUtils.loadTDBData(getShapesModelIRI(), getShapeRdfFile());
                RdfUtils.insertPrefix(getShapesModelIRI(), "shape", getShapesPrefixURL());
                RdfUtils.insertPrefix(getShapesModelIRI(), "stat", getShapesStatsPrefixURL());
                System.out.println("Size of the new shape model " + getShapesModelIRI() + " = " + RdfUtils.getSizeOfModel(getShapesModelIRI()));
            }
        }
    }
    
    private static void loadSHACL() {
        if (Objects.equals(ConfigManager.getProperty("loadShapesData"), "true")) {
            RdfUtils.loadTDBData(getShapesModelIRI(), getShapeRdfFile());
            RdfUtils.insertPrefix(getShapesModelIRI(), "shape", getShapesPrefixURL());
            RdfUtils.insertPrefix(getShapesModelIRI(), "stat", getShapesStatsPrefixURL());
            
        } else {
            System.out.println("Size of the model " + getShapesModelIRI() + " = " + RdfUtils.getSizeOfModel(getShapesModelIRI()));
        }
    }
    
    private static void loadRDF() {
        if (Objects.equals(ConfigManager.getProperty("loadRdfData"), "true")) {
            RdfUtils.loadTDBData(getRdfModelIRI(), getRdfFile());
        } else {
            //System.out.println("Size of the model " + getRdfModelIRI() + " = " + RdfUtils.getSizeOfModel(getRdfModelIRI()));
            System.out.println("Size of the model " + getRdfModelIRI() + " would be too big, so let's avoid it.");
        }
    }
    
    private static void loadRdfFromMultipleFiles() {
        // in case there are multiple NT files to be loaded in the Jena TDB
        if (Boolean.parseBoolean(ConfigManager.getProperty("loadRdfDataFromFolder"))) {
            final File folder = new File(Objects.requireNonNull(ConfigManager.getProperty("rdfFilesFolder")));
            for (final File fileEntry : Objects.requireNonNull(folder.listFiles())) {
                if (FilenameUtils.getExtension(fileEntry.getName()).equals("nt") || FilenameUtils.getExtension(fileEntry.getName()).equals("ttl")) {
                    String rdfFile = fileEntry.getPath();
                    System.out.println(rdfFile);
                    int pos = rdfFile.lastIndexOf("/");
                    String queryId = rdfFile.substring(pos >= 0 ? (pos + 1) : 0);
                    System.out.println(queryId);
                    RdfUtils.loadTDBDataFromNtFormatFiles(getRdfModelIRI(), rdfFile);
                }
            }
        }
    }
    
    private static void getGlobalStatisticsByQuerying() {
        //In this method we will get stats about the whole RDF graph
        //Distinct objects
        distinctObjectsCount = (RdfUtils.runAQuery("SELECT (COUNT(DISTINCT ?o) as ?distinctObjects) WHERE { ?s ?p ?o }",
                getRdfModelIRI()).next().getLiteral("distinctObjects")).getInt();
        
        //Distinct subjects:
        distinctSubjectsCount = (RdfUtils.runAQuery("SELECT (COUNT(DISTINCT ?s) as ?distinctSubjects) WHERE { ?s ?p ?o }",
                getRdfModelIRI()).next().getLiteral("distinctSubjects")).getInt();
        
        
        //All triples
        allTriplesCount = (RdfUtils.runAQuery("SELECT (COUNT(DISTINCT *) as ?allTriples) WHERE { ?s ?p ?o }",
                getRdfModelIRI()).next().getLiteral("allTriples")).getInt();
        
        
        //Distinct rdf:type count
        distinctRdfTypeCount = (RdfUtils.runAQuery("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> SELECT (COUNT(DISTINCT *) as ?distinctRdfType) WHERE { ?s ?p ?o . FILTER(?p=rdf:type) }",
                getRdfModelIRI()).next().getLiteral("distinctRdfType")).getInt();
        
        //Distinct rdf:type count
        distinctRdfTypeObjectsCount = (RdfUtils.runAQuery("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> SELECT (COUNT(DISTINCT ?o) as ?distinctRDFObjectCount) WHERE { ?s a ?o }",
                getRdfModelIRI()).next().getLiteral("distinctRDFObjectCount")).getInt();
        
        //Distinct subject rdf:type count
        distinctRdfTypeSubjCount = (RdfUtils.runAQuery("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> SELECT (COUNT(DISTINCT ?s) as ?distinctSubj) WHERE { ?s a ?o }",
                getRdfModelIRI()).next().getLiteral("distinctSubj")).getInt();
        System.out.println(allTriplesCount + " - " + distinctObjectsCount + " - " + distinctSubjectsCount + " - " + distinctRdfTypeCount + " - " + distinctRdfTypeSubjCount + " - " + distinctRdfTypeObjectsCount);
        
    }
    
    private static void setGlobalStatistics() {
        switch (dataset) {
            case "LUBM":
                allTriplesCount = 91108733;
                distinctObjectsCount = 12253331;
                distinctSubjectsCount = 10847184;
                distinctRdfTypeCount = 25101580;
                distinctRdfTypeSubjCount = 10847184;
                distinctRdfTypeObjectsCount = 46;
                break;
            
            case "WATDIV":
                //For WatDiv 100M Dataset
                allTriplesCount = 108997714;
                distinctObjectsCount = 9753266;
                distinctSubjectsCount = 5212385;
                distinctRdfTypeCount = 1359262;
                distinctRdfTypeSubjCount = 1250145;
                distinctRdfTypeObjectsCount = 39;
                
                //For WatDiv 1 Billion Dataset
                /*allTriplesCount = 1092155948;
                distinctObjectsCount = 92220397;
                distinctSubjectsCount = 52120385;
                distinctRdfTypeCount = 13588993;
                distinctRdfTypeSubjCount = 12500145;
                distinctRdfTypeObjectsCount = 39;*/
                break;
            
            case "YAGO":
                allTriplesCount = 210162514;
                distinctObjectsCount = 126648227;
                distinctSubjectsCount = 5878784;
                distinctRdfTypeCount = 17276993;
                distinctRdfTypeSubjCount = 5878655;
                distinctRdfTypeObjectsCount = 8912;
                break;
            default:
                System.out.println("You must provide the dataset name!");
        }
    }
    
    private static void generateShapesStatistics() {
        try {
            StatsGenerator statsGenerator = new StatsGenerator(getRdfModelIRI(), getShapesModelIRI());
            statsGenerator.executeStatsGenerator();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static String getRdfFile() {
        return ConfigManager.getProperty("rdfFile");
    }
    
    private static String getShapeRdfFile() {
        return ConfigManager.getProperty("shapesFile");
    }
    
    public static String getShapesPrefixURL() {
        return ConfigManager.getProperty("shapesPrefixURL");
    }
    
    public static String getShapesStatsPrefixURL() {
        return ConfigManager.getProperty("shapesStatsPrefixURL");
    }
    
    public static String getRdfModelIRI() {
        return ConfigManager.getProperty("rdfModelIRI");
    }
    
    public static String getShapesModelIRI() {
        if (Boolean.parseBoolean(ConfigManager.getProperty("globalStatsExec")))
            return ConfigManager.getProperty("shapesModelIRI") + "void/";
        else return ConfigManager.getProperty("shapesModelIRI");
    }
    
    public static String getEndPointAddress() {
        return ConfigManager.getProperty("fusekiSparqlEndPointAddress");
    }
    
    /*private static void csv(String fileName, HashMap<String, Tuple5<Long, Long, Long, Long, Integer>> executionTime) {
        FileWriter csvWriter = null;
        try {
            csvWriter = new FileWriter(fileName);
            csvWriter.append("QueryID");
            csvWriter.append(",");
            csvWriter.append("Shape_Planning_Time_MS");
            csvWriter.append(",");
            csvWriter.append("Shape_Execution_Time_MS");
            csvWriter.append(",");
            csvWriter.append("Jena_Planning_Time_MS");
            csvWriter.append(",");
            csvWriter.append("Jena_Execution_Time_MS");
            csvWriter.append(",");
            csvWriter.append("Result_Size");
            csvWriter.append("\n");
            
            List<List<String>> rows = new ArrayList<>();
            
            executionTime.forEach((key, value) -> {
                List<String> temp = new ArrayList<>();
                temp.add(key);
                temp.add(value._1.toString());
                temp.add(value._2.toString());
                temp.add(value._3.toString());
                temp.add(value._4.toString());
                temp.add(value._5.toString());
                
                rows.add(temp);
            });
            
            for (List<String> rowData : rows) {
                csvWriter.append(String.join(",", rowData));
                csvWriter.append("\n");
            }
            csvWriter.flush();
            csvWriter.close();
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    */
    public static int getAllTriplesCount() {
        return allTriplesCount;
    }
    
    public static int getDistinctObjectsCount() {
        return distinctObjectsCount;
    }
    
    public static int getDistinctSubjectsCount() {
        return distinctSubjectsCount;
    }
    
    public static int getDistinctRdfTypeCount() {
        return distinctRdfTypeCount;
    }
    
    public static int getDistinctRdfTypeSubjCount() {
        return distinctRdfTypeSubjCount;
    }
    
    public static int getDistinctRdfTypeObjectsCount() {
        return distinctRdfTypeObjectsCount;
    }
}
