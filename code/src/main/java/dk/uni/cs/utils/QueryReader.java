package dk.uni.cs.utils;

import dk.uni.cs.utils.ConfigManager;
import org.apache.commons.io.FilenameUtils;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Objects;

public class QueryReader {
    private final File folder = new File(Objects.requireNonNull(ConfigManager.getProperty("queries_directory")));
    private HashMap<String, Query> queriesWithID = new HashMap<>();

    public QueryReader(){
        readQueries();
        //readInsertQueries();
    }

    public HashMap<String, Query> getQueriesWithID() {
        return queriesWithID;
    }


    public void readQueries(){
        for (final File fileEntry : Objects.requireNonNull(this.folder.listFiles())) {

            if(FilenameUtils.getExtension(fileEntry.getName()).equals("sparql") || FilenameUtils.getExtension(fileEntry.getName()).equals("txt")){
                String queryFile = fileEntry.getPath();
                System.out.println(queryFile);
                int pos = queryFile.lastIndexOf("/");
                String queryId = queryFile.substring(pos >= 0 ? (pos + 1) : 0);
                Query query = QueryFactory.read(queryFile);
                queriesWithID.put(queryId, query);
            }
        }
    }


    public void readInsertQueries(){
        /*GraphDBUtils graphDBUtils = new GraphDBUtils();
        for (final File fileEntry : Objects.requireNonNull(this.folder.listFiles())) {

            //if(FilenameUtils.getExtension(fileEntry.getName()).equals("sparql") || FilenameUtils.getExtension(fileEntry.getName()).equals("txt")){
                String queryFile = fileEntry.getPath();
                int pos = queryFile.lastIndexOf("/");
                String queryId = queryFile.substring(pos >= 0 ? (pos + 1) : 0);

                Path fileName = Path.of(queryFile);
                String query = "";
                System.out.println(queryId);
                //TODO Fix the issue of having such brackets () in the IRI's e.g. https://yago-knowledge.org/resource/Library_(computing)
                try {
                    query = Files.readString(fileName);
                    query = query.replaceAll("\\(", "\\\\(").replaceAll("\\)","\\\\)").replaceAll("'", "%27").replaceAll("&","\\\\&").replaceAll("–", "%E2%80%93");
                    System.out.println(query);
                    query = "PREFIX shape: <http://yago.shacl.io/>\n" +
                            "PREFIX stat: <http://yago.shacl.stat.io/>\n" +
                            "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                            "PREFIX sh: <http://www.w3.org/ns/shacl#>" + query;
                    graphDBUtils.updateQueryExecutor(query);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            //}
        }*/
    }
}
