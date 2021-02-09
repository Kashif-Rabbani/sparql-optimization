package dk.uni.cs.shapes;

import dk.uni.cs.Main;
import dk.uni.cs.utils.ConfigManager;
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

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class StatsGenerator {
    private final String shape = Main.getShapesPrefixURL(); //shapesPrefixURL
    private final String shapesStatsPrefixURL = Main.getShapesStatsPrefixURL();
    private String graphModelIRI = null;
    private String shapesModelIRI = null;
    private List<String> statisticQueries = new ArrayList<>();

    public StatsGenerator(String graphModelIRI, String shapesModelIRI) {
        this.graphModelIRI = graphModelIRI;
        this.shapesModelIRI = shapesModelIRI;
    }

    public void executeStatsGenerator() {
        Dataset dataset = RdfUtils.getTDBDataset();
        dataset.begin(ReadWrite.READ);
        try {
            Model graphModel = dataset.getNamedModel(graphModelIRI);
            captureStats(graphModel);

        } finally {
            dataset.commit();
            dataset.end();
        }

        applyStats(shapesModelIRI);
        dataset.close();
    }

    private void captureStats(Model graphModel) {
        System.out.println("Preparing Stats...");
        String classesQuery = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" + "SELECT DISTINCT ?className  WHERE { ?s rdf:type ?className. }";
        System.out.println(classesQuery);
        // Get Classes
        RdfUtils.runAQuery(classesQuery, graphModel).forEachRemaining(triple -> {
            prepareStats(graphModel, triple);
        });
    }

    private void prepareStats(Model graphModel, QuerySolution classTypeTriple) {
        String countPropClassQuery = "SELECT ?property (Count(*) AS ?count) WHERE " + "{ [] a <" + classTypeTriple.get("className") + "> ; ?property [] .} GROUP BY ?property ";
        System.out.println(countPropClassQuery);
        // Get Properties with their count
        Objects.requireNonNull(RdfUtils.runAQuery(countPropClassQuery, graphModel)).forEachRemaining(countPropTriple -> {
            AtomicReference<String> maxCountPropResult = new AtomicReference<>("");
            AtomicReference<String> minCountPropResult = new AtomicReference<>("");
            AtomicReference<String> distinctCountPropResult = new AtomicReference<>("");

            Objects.requireNonNull(RdfUtils.runAQuery(getMaxCountQuery(classTypeTriple, countPropTriple), graphModel)).forEachRemaining(val -> {
                maxCountPropResult.set(val.get("maxCount").asLiteral().getInt() + "\"" + "^^xsd:integer");
            });


            Objects.requireNonNull(RdfUtils.runAQuery(getMinCountQuery(classTypeTriple, countPropTriple), graphModel)).forEachRemaining(val -> {
                minCountPropResult.set(val.get("minCount").asLiteral().getInt() + "\"" + "^^xsd:integer");
            });


            //Get the answer i.e., count of distinct properties for the above query
            Objects.requireNonNull(RdfUtils.runAQuery(getDistinctPropertyCountQuery(classTypeTriple, countPropTriple), graphModel)).forEachRemaining(dct -> {
                distinctCountPropResult.set(dct.get("distinctCount").asLiteral().getInt() + "\"" + "^^xsd:integer");
            });

            prepareInsertQueriesForShapeProperties(classTypeTriple, countPropTriple, maxCountPropResult, distinctCountPropResult, minCountPropResult);
        });
        prepareInsertQueriesForNodeShapes(graphModel, classTypeTriple);

    }

    private void prepareInsertQueriesForNodeShapes(Model graphModel, QuerySolution classTypeTriple) {
        /**
         * I have figured it out that rdf:type properties should be handled differently. For example:  This query
         * SELECT (Count(*) AS ?count) WHERE { [] a ub:ResearchAssistant ; a [] .}
         * returns the total count of rdf:type property which involves the other rdf:type counts such as ub:GraduateStudent
         * To accurately count the rdf:type triples for one specific class, this query should be used: SELECT (Count(*) AS ?rdfCount) WHERE { ?x a ub:GraduateStudent .}
         */

        System.out.println("-------------------- printing queries -------------------");
        String rdfCountQuery = "SELECT (Count(*) AS ?rdfCount) WHERE { ?x a <" + classTypeTriple.get("className") + ">  .}";
        System.out.println(rdfCountQuery);
        int rdfCount = Objects.requireNonNull(RdfUtils.runAQuery(rdfCountQuery, graphModel)).next().get("rdfCount").asLiteral().getInt();


        String rdfDistinctCountQuery = "SELECT (Count(DISTINCT *) AS ?rdfCount) WHERE { ?x a <" + classTypeTriple.get("className") + ">  .}";
        System.out.println(rdfDistinctCountQuery);
        int rdfDistinctCount = Objects.requireNonNull(RdfUtils.runAQuery(rdfDistinctCountQuery, graphModel)).next().get("rdfCount").asLiteral().getInt();

        String shapeCountQuery = "\n" +
                "INSERT {\n" +
                "shape:" + classTypeTriple.getResource("className").getLocalName() + "Shape" + " stat:count  " + "\"" + rdfCount + "\"" + "^^xsd:integer" + " . \n" +
                "shape:" + classTypeTriple.getResource("className").getLocalName() + "Shape" + " stat:distinctCount  " + "\"" + rdfDistinctCount + "\"" + "^^xsd:integer" + " . \n" +
                "} \n" +
                "WHERE {\n" +
                "shape:" + classTypeTriple.getResource("className").getLocalName() + "Shape" + " a sh:NodeShape .\n" +
                "}";
        System.out.println(shapeCountQuery);
        statisticQueries.add(shapeCountQuery);
    }

    private void prepareInsertQueriesForShapeProperties(QuerySolution triple, QuerySolution t, AtomicReference<String> maxCountPropResult, AtomicReference<String> distinctCountPropResult, AtomicReference<String> minCountPropResult) {
        if (!t.getResource("property").equals(RDF.type)) {
            String shapePropCountQuery = "\n" +
                    "INSERT {\n" +
                    "shape:" + t.getResource("property").getLocalName() + triple.getResource("className").getLocalName() + "ShapeProperty" + " stat:count  " + "\"" + t.get("count").asLiteral().getInt() + "\"" + "^^xsd:integer" + " . \n" +
                    "shape:" + t.getResource("property").getLocalName() + triple.getResource("className").getLocalName() + "ShapeProperty" + " stat:distinctCount  " + "\"" + distinctCountPropResult.get() + " . \n" +
                    "shape:" + t.getResource("property").getLocalName() + triple.getResource("className").getLocalName() + "ShapeProperty" + " stat:maxCount  " + "\"" + maxCountPropResult.get() + " . \n" +
                    "shape:" + t.getResource("property").getLocalName() + triple.getResource("className").getLocalName() + "ShapeProperty" + " stat:minCount  " + "\"" + minCountPropResult.get() + " . \n" +

                    "} \n" +
                    "WHERE {\n" +
                    "shape:" + triple.getResource("className").getLocalName() + "Shape" + " a sh:NodeShape; sh:property " + "shape:" + t.getResource("property").getLocalName() + triple.getResource("className").getLocalName() + "ShapeProperty  . \n" +
                    "}";
            statisticQueries.add(shapePropCountQuery);
        }
    }

    //Query to get the distinct number of triples for the given property
    private String getDistinctPropertyCountQuery(QuerySolution classTypeTriple, QuerySolution countPropTriple) {
        return "SELECT (COUNT( DISTINCT ?prop) AS ?distinctCount )  " +
                "WHERE { [] a <" + classTypeTriple.get("className") + "> ; <" + countPropTriple.get("property") + "> ?prop . }";
    }

    private String getMaxCountQuery(QuerySolution classTypeTriple, QuerySolution countPropTriple) {
        String query = "SELECT (MAX(?Y) AS ?maxCount) WHERE {\n" +
                "SELECT (COUNT( DISTINCT ?x) AS ?Y ) \n" +
                "WHERE {\n" +
                "    [] a <" + classTypeTriple.get("className") + "> ; <" + countPropTriple.get("property") + "> ?x .\n" +
                "}\n" +
                "    GROUP BY ?x \n" +
                "}";
        System.out.println(query);
        return query;
    }

    private String getMinCountQuery(QuerySolution classTypeTriple, QuerySolution countPropTriple) {
        return "SELECT (MIN(?Y) AS ?minCount) WHERE {\n" +
                "SELECT (COUNT( DISTINCT ?x) AS ?Y ) \n" +
                "WHERE {\n" +
                "    [] a <" + classTypeTriple.get("className") + "> ; <" + countPropTriple.get("property") + "> ?x .\n" +
                "}\n" +
                "    GROUP BY ?x \n" +
                "}";
    }

    private void applyStats(String shapesModelIRI) {
        System.out.println("Annotating stats into shapes graph....");
        String prefixes = RdfUtils.extractPrefixMappingsForQuery(Main.getRdfModelIRI(), Main.getShapesModelIRI());

        statisticQueries.forEach((query) -> {
            System.out.println(query);
            //RdfUtils.runInsertQuery(prefixes + query, shapesModelIRI);
        });
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


















