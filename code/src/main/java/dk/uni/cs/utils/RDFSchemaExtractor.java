package dk.uni.cs.utils;

import org.apache.commons.collections4.ListUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class RDFSchemaExtractor {
    String fileAddress = ConfigManager.getProperty("resourceFile");
    List<String> classes = new ArrayList<String>();
    
    public RDFSchemaExtractor() {
        fileReader();
        List[] x = partition(classes, classes.size()/10);
        AtomicInteger counter = new AtomicInteger();
        Arrays.stream(x).iterator().forEachRemaining(list -> {
            //System.out.println(list.size());
            new Runnable( "Thread " + counter, list).start();
            counter.getAndIncrement();
        });
    }
    
    public static void executeQueryOnGraphDB(String classURI) {
        GraphDBUtils graphDBUtils = new GraphDBUtils();
        // This query extracts all the classes and their properties no matter what's the size of the dataset
        //Daniel helped me to create this query.
     
        String query = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "PREFIX schema: <http://schema.org/>\n" +
                "PREFIX yago: <http://yago-knowledge.org/resource/>\n" +
                "CONSTRUCT {\n" +
                "    ?new_instance a ?class ;\n" +
                "                  ?property ?new_property_value .\n" +
                "}\n" +
                "WHERE {\n" +
                "    SELECT DISTINCT * WHERE { \n" +
                "        ?instance a ?class ;\n" +
                "                  ?property ?property_value .\n" +
                "        FILTER (?class = <" + classURI + "> ) .\n" +
                "        FILTER (rdf:type!=?property)\n" +
                "        BIND (URI(concat(str(?class), \"/instance\")) as ?new_instance)\n" +
                "        BIND (coalesce(datatype(?property_value), URI(concat(str(?property), \"/value\"))) as ?new_property_value)\n" +
                "    }\n" +
                "\n" +
                "} \n" +
                "\n" +
                "\n";
        System.out.println(query);
        graphDBUtils.runGraphQuery(query, ConfigManager.getProperty("resourcesPath") + "data.nt");
    }
    
    private void fileReader() {
        try {
            
            File f = new File(fileAddress);
            
            BufferedReader b = new BufferedReader(new FileReader(f));
            
            String readLine = "";
            
            System.out.println("Reading file using Buffered Reader");
            
            while ((readLine = b.readLine()) != null) {
                classes.add(readLine);
            }
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    // Generic function to partition a list into sublists of size n each in Java
    // using Apache Common Collections (The final list might have less items)
    public static <T> List[] partition(List<T> list, int n) {
        // calculate number of partitions of size n each
        int m = list.size() / n;
        if (list.size() % n != 0)
            m++;
        
        // partition a list into sublists of size n each
        List<List<T>> itr = ListUtils.partition(list, n);
        
        // create m empty lists and initialize it with sublists
        ArrayList[] partition = new ArrayList[m];
        for (int i = 0; i < m; i++)
            partition[i] = new ArrayList<>(itr.get(i));
        
        // return the lists
        return partition;
    }
}
