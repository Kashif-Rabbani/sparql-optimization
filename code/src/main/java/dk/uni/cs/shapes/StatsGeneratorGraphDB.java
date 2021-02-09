package dk.uni.cs.shapes;

import dk.uni.cs.Main;
import dk.uni.cs.utils.ConfigManager;
import dk.uni.cs.utils.GraphDBUtils;
import dk.uni.cs.utils.RdfUtils;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.impl.PropertyImpl;
import org.apache.jena.rdf.model.impl.ResourceImpl;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.vocabulary.RDF;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.BindingSet;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class StatsGeneratorGraphDB {
    //private final String shape = Main.getShapesPrefixURL(); //shapesPrefixURL
    //private final String shapesStatsPrefixURL = Main.getShapesStatsPrefixURL();
    //private String graphModelIRI = null;
    //private String shapesModelIRI = null;
    private List<String> statisticQueries = new ArrayList<>();
    private GraphDBUtils graphDBUtils = new GraphDBUtils();
    
    public StatsGeneratorGraphDB() {
        gatherStats();
        applyStats();
    }
    
    public void gatherStats() {
        String classesQuery = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" + "SELECT DISTINCT ?className  WHERE { ?s rdf:type ?className. }";
        List<BindingSet> allClasses = graphDBUtils.runSelectQuery(classesQuery);
        allClasses.forEach(triple -> {
            System.out.println(triple.getValue("className").stringValue());
            prepareStats(triple);
            //System.out.println(graphDBUtils.getValueFactory().createIRI(triple.getValue("className").stringValue()).getLocalName());
        });
    }
    
    private void prepareStats(BindingSet classTypeTriple) {
        String countPropClassQuery = "SELECT ?property (Count(*) AS ?count) WHERE " + "{ [] a <" + classTypeTriple.getValue("className").stringValue() + "> ; ?property [] .} GROUP BY ?property ";
        System.out.println(countPropClassQuery);
        
        // Get Properties with their count
        graphDBUtils.runSelectQuery(countPropClassQuery).forEach(countPropTriple -> {
            AtomicReference<String> maxCountPropResult = new AtomicReference<>("");
            AtomicReference<String> minCountPropResult = new AtomicReference<>("");
            AtomicReference<String> distinctCountPropResult = new AtomicReference<>("");
            
            graphDBUtils.runSelectQuery(getMaxCountQuery(classTypeTriple, countPropTriple)).forEach(val -> {
                maxCountPropResult.set(Integer.parseInt(val.getValue("maxCount").stringValue()) + "\"" + "^^xsd:integer");
            });
            
            graphDBUtils.runSelectQuery(getMinCountQuery(classTypeTriple, countPropTriple)).forEach(val -> {
                minCountPropResult.set(Integer.parseInt(val.getValue("minCount").stringValue()) + "\"" + "^^xsd:integer");
            });
            
            graphDBUtils.runSelectQuery(getDistinctPropertyCountQuery(classTypeTriple, countPropTriple)).forEach(val -> {
                distinctCountPropResult.set(Integer.parseInt(val.getValue("distinctCount").stringValue()) + "\"" + "^^xsd:integer");
            });
            
            IRI classTypeTripleIRI = graphDBUtils.getValueFactory().createIRI(classTypeTriple.getValue("className").stringValue());
            IRI countTripleIRI = graphDBUtils.getValueFactory().createIRI(countPropTriple.getValue("property").stringValue());
            prepareInsertQueriesForShapeProperties(classTypeTriple, classTypeTripleIRI, countPropTriple, countTripleIRI, maxCountPropResult, distinctCountPropResult, minCountPropResult);
            
        });// ends here
        prepareInsertQueriesForNodeShapes(classTypeTriple, graphDBUtils.getValueFactory().createIRI(classTypeTriple.getValue("className").stringValue()));
        
    }
    
    
    private void prepareInsertQueriesForNodeShapes(BindingSet classTypeTriple, IRI classTypeTripleIRI) {
        /**
         * I have figured it out that rdf:type properties should be handled differently. For example:  This query
         * SELECT (Count(*) AS ?count) WHERE { [] a ub:ResearchAssistant ; a [] .}
         * returns the total count of rdf:type property which involves the other rdf:type counts such as ub:GraduateStudent
         * To accurately count the rdf:type triples for one specific class, this query should be used: SELECT (Count(*) AS ?rdfCount) WHERE { ?x a ub:GraduateStudent .}
         */
        
        String rdfCountQuery = "SELECT (Count(*) AS ?rdfCount) WHERE { ?x a <" + classTypeTriple.getValue("className").stringValue() + ">  .}";
        String rdfDistinctCountQuery = "SELECT (Count(DISTINCT *) AS ?rdfCount) WHERE { ?x a <" + classTypeTriple.getValue("className").stringValue() + ">  .}";
        
        int rdfCount = Integer.parseInt(graphDBUtils.runSelectQuery(rdfCountQuery).get(0).getValue("rdfCount").stringValue());
        int rdfDistinctCount = Integer.parseInt(graphDBUtils.runSelectQuery(rdfDistinctCountQuery).get(0).getValue("rdfCount").stringValue());
        
        
        String shapeCountQuery = "\n" +
                "INSERT {\n" +
                "shape:" + classTypeTripleIRI.getLocalName() + "Shape" + " stat:count  " + "\"" + rdfCount + "\"" + "^^xsd:integer" + " . \n" +
                "shape:" + classTypeTripleIRI.getLocalName() + "Shape" + " stat:distinctCount  " + "\"" + rdfDistinctCount + "\"" + "^^xsd:integer" + " . \n" +
                "} \n" +
                "WHERE {\n" +
                "shape:" + classTypeTripleIRI.getLocalName() + "Shape" + " a sh:NodeShape .\n" +
                "}";
        System.out.println(shapeCountQuery);
        statisticQueries.add(shapeCountQuery);
    }
    
    private void prepareInsertQueriesForShapeProperties(BindingSet triple, IRI tripleIRI, BindingSet t, IRI tIRI, AtomicReference<String> maxCountPropResult, AtomicReference<String> distinctCountPropResult, AtomicReference<String> minCountPropResult) {
        if (!t.getValue("property").stringValue().equals(RDF.type.toString())) {
            String shapePropCountQuery = "\n" +
                    "INSERT {\n" +
                    "shape:" + tIRI.getLocalName() + tripleIRI.getLocalName() + "ShapeProperty" + " stat:count  " + "\"" + Integer.parseInt(t.getValue("count").stringValue()) + "\"" + "^^xsd:integer" + " . \n" +
                    "shape:" + tIRI.getLocalName() + tripleIRI.getLocalName() + "ShapeProperty" + " stat:distinctCount  " + "\"" + distinctCountPropResult.get() + " . \n" +
                    "shape:" + tIRI.getLocalName() + tripleIRI.getLocalName() + "ShapeProperty" + " stat:maxCount  " + "\"" + maxCountPropResult.get() + " . \n" +
                    "shape:" + tIRI.getLocalName() + tripleIRI.getLocalName() + "ShapeProperty" + " stat:minCount  " + "\"" + minCountPropResult.get() + " . \n" +
                    "} \n" +
                    "WHERE {\n" +
                    "shape:" + tripleIRI.getLocalName() + "Shape" + " a sh:NodeShape; sh:property " + "shape:" + tIRI.getLocalName() + tripleIRI.getLocalName() + "ShapeProperty  . \n" +
                    "}";
            statisticQueries.add(shapePropCountQuery);
        }
    }
    
    //Query to get the distinct number of triples for the given property
    private String getDistinctPropertyCountQuery(BindingSet classTypeTriple, BindingSet countPropTriple) {
        return "SELECT (COUNT( DISTINCT ?prop) AS ?distinctCount )  " +
                "WHERE { [] a <" + classTypeTriple.getValue("className").stringValue() + "> ; <" + countPropTriple.getValue("property").stringValue() + "> ?prop . }";
    }
    
    private String getMaxCountQuery(BindingSet classTypeTriple, BindingSet countPropTriple) {
        String query = "SELECT (MAX(?Y) AS ?maxCount) WHERE {\n" +
                "SELECT (COUNT( DISTINCT ?x) AS ?Y ) \n" +
                "WHERE {\n" +
                "    [] a <" + classTypeTriple.getValue("className").stringValue() + "> ; <" + countPropTriple.getValue("property").stringValue() + "> ?x .\n" +
                "}\n" +
                "}";
        System.out.println(query);
        return query;
    }
    
    private String getMinCountQuery(BindingSet classTypeTriple, BindingSet countPropTriple) {
        return "SELECT (MIN(?Y) AS ?minCount) WHERE {\n" +
                "SELECT (COUNT( DISTINCT ?x) AS ?Y ) \n" +
                "WHERE {\n" +
                "    [] a <" + classTypeTriple.getValue("className").toString() + "> ; <" + countPropTriple.getValue("property").toString() + "> ?x .\n" +
                "}\n" +
                "}";
    }
    
    private void applyStats() {
        System.out.println("Applying Statistics");
        int index = 0;
        System.out.println("Printing Insert Queries");
        for (String query : statisticQueries) {
            System.out.println(query);
            System.out.println("----");
        }
        
        System.out.println("Writing Queries into One File");
        for (String query : statisticQueries) {
            RdfUtils.writeToFileInAppendMode(query, ConfigManager.getProperty("pathForSavingInsertQueries") + "allQueries.txt");
        }
        System.out.println("Writing queries into separate files");
        for (String query : statisticQueries) {
            RdfUtils.writeToFile(query, ConfigManager.getProperty("pathForSavingInsertQueries") + "InsertQueries/" + index + ".txt");
            index++;
        }
        System.out.println("Writing into the Shapes Model " + Main.getShapesModelIRI());
        for (String query : statisticQueries) {
            RdfUtils.insertWatDivPrefixes(Main.getShapesModelIRI());
            String prefixes = RdfUtils.extractPrefixMappingsForQuery(Main.getRdfModelIRI(), Main.getShapesModelIRI());
            RdfUtils.runInsertQuery(prefixes + query, Main.getShapesModelIRI());
        }
    }
    
    //This method is to generate a shape having its properties with actual distinct subject and object count
    public static void addingCustomModel() {
        
        Map<String, String> prefixes = new HashMap<>();
        prefixes.put("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
        prefixes.put("ub", "http://swat.cse.lehigh.edu/onto/univ-bench.owl#");
        prefixes.put("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
        prefixes.put("stat", "http://lubm.shacl.stat.io/");
        prefixes.put("sh", "http://www.w3.org/ns/shacl#");
        prefixes.put("shape", "http://lubm.shacl.io/");
        prefixes.put("xsd", "http://www.w3.org/2001/XMLSchema#");
        
        
        Model shapeModel = ModelFactory.createDefaultModel();
        shapeModel.setNsPrefixes(prefixes);
        List<Statement> statements = new ArrayList<>();
        
        statements.add(ResourceFactory.createStatement(new ResourceImpl(Main.getShapesPrefixURL() + "GraphShape"),
                RDF.type, new ResourceImpl("http://www.w3.org/ns/shacl#NodeShape")));
        
        String query = "SELECT DISTINCT ?p (COUNT(DISTINCT ?s) as ?distinctSubject) (COUNT(DISTINCT ?o) as ?distinctObject) WHERE { ?s ?p ?o} GROUP BY  ?p";
        
        ResultSet result = RdfUtils.runAQuery(query, Main.getRdfModelIRI());
        while (result.hasNext()) {
            QuerySolution row = result.next();
            
            if (row.get("p").asResource().getNameSpace().equals(ConfigManager.getProperty("datasetPrefixNameSpace"))) {
                statements.add(ResourceFactory.createStatement(
                        new ResourceImpl(Main.getShapesPrefixURL() + "GraphShape"),
                        new PropertyImpl("http://www.w3.org/ns/shacl#property"),
                        new ResourceImpl(Main.getShapesPrefixURL() + row.get("p").asResource().getLocalName())));
                
                statements.add(ResourceFactory.createStatement(
                        new ResourceImpl(Main.getShapesPrefixURL() + row.get("p").asResource().getLocalName()),
                        RDF.type,
                        new ResourceImpl("http://www.w3.org/ns/shacl#PropertyShape")));
                
                statements.add(ResourceFactory.createStatement(
                        new ResourceImpl(Main.getShapesPrefixURL() + row.get("p").asResource().getLocalName()),
                        new PropertyImpl("http://www.w3.org/ns/shacl#nodeKind"),
                        new ResourceImpl("http://www.w3.org/ns/shacl#IRI")));
                
                statements.add(ResourceFactory.createStatement(
                        new ResourceImpl(Main.getShapesPrefixURL() + row.get("p").asResource().getLocalName()),
                        new PropertyImpl("http://www.w3.org/ns/shacl#path"),
                        row.get("p").asResource()
                ));
                
                statements.add(ResourceFactory.createStatement(
                        new ResourceImpl(Main.getShapesPrefixURL() + row.get("p").asResource().getLocalName()),
                        new PropertyImpl(Main.getShapesStatsPrefixURL() + "distinctSubjectCount"),
                        row.get("distinctSubject").asLiteral()
                ));
                
                statements.add(ResourceFactory.createStatement(
                        new ResourceImpl(Main.getShapesPrefixURL() + row.get("p").asResource().getLocalName()),
                        new PropertyImpl(Main.getShapesStatsPrefixURL() + "distinctObjectCount"),
                        row.get("distinctObject").asLiteral()
                ));
            }
            
        }
        
        shapeModel.add(statements);
        
        // Write Turtle to the blocks variant
        //RDFDataMgr.write(System.out, shapeModel, RDFFormat.TURTLE_PRETTY) ;
        
        try {
            shapeModel.write(new FileOutputStream("shapeFile" + ".ttl"), "TTL");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        
    }
}

















