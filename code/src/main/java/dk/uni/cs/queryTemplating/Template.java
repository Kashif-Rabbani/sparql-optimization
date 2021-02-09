package dk.uni.cs.queryTemplating;
import dk.uni.cs.utils.RdfUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class Template {
}


class ReadFileLineByLineUsingFiles {
    
    public static void main(String[] args) {
        String query = "SELECT * WHERE {\n" +
                "\t?v0 <http://schema.org/caption> ?v1 .\n" +
                "\t?v0 <http://schema.org/text> ?v2 .\n" +
                "\t?v0 <http://schema.org/contentRating> ?v3 .\n" +
                "\t?v0 <http://purl.org/stuff/rev#hasReview> ?v4 .\n" +
                "\t?v4 <http://purl.org/stuff/rev#title> ?v5 .\n" +
                "\t?v4 <http://purl.org/stuff/rev#reviewer> ?v6 .\n" +
                "\t?v7 <http://schema.org/actor> ?v6 .\n" +
                "\t?v7 <http://schema.org/language> ?v8 .\n" +
                "?v0 a ?X .\n" +
                "\n" +
                "}";
        try {
            List<String> allLines = Files.readAllLines(Paths.get("src/main/resources/watDiv_resources/Others/templates.txt"));
            int i = 22;
            for (String line : allLines) {
                System.out.println(line);
                String x = query.replaceAll(" \\?X ", " <"+line+"> ");
                System.out.println(x);
    
                RdfUtils.writeToFile(x,"src/main/resources/watDiv_resources/watDivQueries/customQueries/"+ "C"+i+".sparql");
                i++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
}