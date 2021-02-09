package dk.uni.cs.shapes;

import dk.uni.cs.Main;
import dk.uni.cs.utils.RdfUtils;
import dk.uni.cs.utils.Tuple3;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDF;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class InMemComputation {
    
    private final String shapesURLPrefix = Main.getShapesPrefixURL();
    
    /**
     * This method gets all the predicates from the SHACL shape graph and retrieves their candidate shape properties
     *
     * @return HashMap<String, List < Tuple3 < String, String, String>>>
     */
    public HashMap<String, List<Tuple3<String, String, String>>> getPredicatesCandidateShapesProp() {
        System.out.println("Invoked: getPredicatesCandidateShapesProp()");
        HashMap<String, List<Tuple3<String, String, String>>> predicateShapeDistMap = new HashMap<>();
        
        //get all distinct predicates -- It doesn't matter if you get it from shacl graph or original data graph, the results would be same
        String queryDistinctProp = "PREFIX sh: <http://www.w3.org/ns/shacl#>\n" +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "SELECT distinct ?prop  WHERE {\n" +
                "   ?shape sh:property ?shapeProperty .\n" +
                "   ?shapeProperty sh:path ?prop .\n" +
                "    FILTER(?prop != rdf:type)\n" +
                "}";
        
        ResultSet x = RdfUtils.runAQueryInMemTDB(queryDistinctProp, Main.getShapesModelIRI());
        while (x.hasNext()) {
            String predicate = x.next().get("prop").toString();
            
            List<Tuple3<String, String, String>> predicateShapes = new ArrayList<>();
            //System.out.println(triple);
            List<Resource> shapeProperties = RdfUtils.getSubjectsFromInMemoryModel("http://www.w3.org/ns/shacl#path", predicate, Main.getShapesModelIRI());
            shapeProperties.forEach(shapeProperty -> {
                Resource r = RdfUtils.getSubjectsFromInMemoryModel("http://www.w3.org/ns/shacl#property", shapeProperty.toString(), Main.getShapesModelIRI()).get(0);
                String nodeShapeLocalName = r.toString().split("/")[(r.toString().split("/").length) - 1];
                
                String query = "SELECT ?classIRI { <" + shapesURLPrefix + nodeShapeLocalName + "> " +
                        "<http://www.w3.org/ns/shacl#targetClass> ?classIRI . FILTER (regex(str(?classIRI), \"" + nodeShapeLocalName.split("Shape")[0] + "\" )) }";
                predicateShapes.add(new Tuple3<String, String, String>(targetClassOfShapeWithLocalName(query), nodeShapeLocalName.split("Shape")[0], shapeProperty.toString()));
            });
            predicateShapeDistMap.put(predicate, predicateShapes);
        }
        return predicateShapeDistMap;
    }
    
    /**
     * This method gets the statistics of the candidate shape properties for the predicates (extracted in the getPredicatesCandidateShapesProp method)
     *
     * @return HashMap<String, List < Statistics>>
     */
    
    //FIXME: Update the DOC of shape properties with the distinct count, and the DSC with the count of Shape.
    //FIXME: This is important because this is a right way to do it.
    public HashMap<String, List<Statistics>> getPredicatesCandidateShapesPropStatistics(HashMap<String, List<Tuple3<String, String, String>>> predicatesToShapesMapping) {
        System.out.println("Invoked: getPredicatesCandidateShapesPropStatistics");
        HashMap<String, List<Statistics>> predicatesWithStats = new HashMap<>();
        //HashMap<String, List<Integer>> shapePropStatsMap =  Main.shapePropStatsMap;
        
        predicatesToShapesMapping.forEach((predicate, list) -> {
            List<Statistics> tempStatList = new ArrayList<Statistics>();
            //System.out.println("Predicate : " + predicate + " : " + list.size() + " - example: " + list.get(0));
            list.forEach(tuple -> {
                if (predicate.equals(RDF.type.toString())) {
                    //TODO : I think this is unnecessary ; it should be removed because it will never be true
                    //System.out.println("InMemComputation: RDF.TYPE");
                    int subjectCount = (RdfUtils.getFromInMemoryModel(tuple._3, Main.getShapesStatsPrefixURL() + "count", Main.getShapesModelIRI())).getInt();
                    Statistics statistics = new Statistics(
                            tuple._3, 1, 1, // default values for min and max count is 1 (Assumption)
                            subjectCount,
                            (RdfUtils.getFromInMemoryModel(tuple._3, Main.getShapesStatsPrefixURL() + "distinctCount", Main.getShapesModelIRI())).getInt(), tuple._2);
                    
                    statistics.setSubject(tuple._1);
                    statistics.setSubjectCount(subjectCount);
                    tempStatList.add(statistics);
                    //long end = System.currentTimeMillis() - start;
                    //System.out.println("RDF.Type : " + end  + " MS") ;
                } else {
                    
                    Statistics statistics;
                    if (Main.shapePropStatsMap.containsKey(tuple._3)) {
                        statistics = new Statistics(
                                tuple._3,
                                Main.shapePropStatsMap.get(tuple._3).get(0),
                                Main.shapePropStatsMap.get(tuple._3).get(1),
                                Main.shapePropStatsMap.get(tuple._3).get(2),
                                Main.shapePropStatsMap.get(tuple._3).get(3),
                                tuple._2);
                        
                    } else {
                        statistics = new Statistics(
                                tuple._3,
                                (RdfUtils.getFromInMemoryModel(tuple._3, Main.getShapesStatsPrefixURL() + "minCount", Main.getShapesModelIRI())).getInt(),
                                (RdfUtils.getFromInMemoryModel(tuple._3, Main.getShapesStatsPrefixURL() + "maxCount", Main.getShapesModelIRI())).getInt(), // need a check on maxCount
                                (RdfUtils.getFromInMemoryModel(tuple._3, Main.getShapesStatsPrefixURL() + "count", Main.getShapesModelIRI())).getInt(),
                                (RdfUtils.getFromInMemoryModel(tuple._3, Main.getShapesStatsPrefixURL() + "distinctCount", Main.getShapesModelIRI())).getInt(), tuple._2);
                        System.out.println("NOT FOUND IN MAP ->  " + tuple._3);
                    }
                    
                    statistics.setSubject(tuple._1);
                    Integer subjectCount = (RdfUtils.getFromInMemoryModel(Main.getShapesPrefixURL() + tuple._2 + "Shape", Main.getShapesStatsPrefixURL() + "count", Main.getShapesModelIRI())).getInt();
                    statistics.setSubjectCount(subjectCount);
                    statistics.setDistinctSubjectCount(subjectCount);
                    
                    // It means that the current property only belongs to the candidate shape RDFGraph
                    if (tuple._2.equals("RDFGraph")) {
                        if (Main.shapePropStatsMap.containsKey(tuple._3)) {
                            //distinct subject count
                            statistics.setDistinctSubjectCount(Main.shapePropStatsMap.get(tuple._3).get(4));
                            
                            //distinct object count
                            statistics.setDistinctObjectCount(Main.shapePropStatsMap.get(tuple._3).get(5));
                        } else {
                            System.out.println("NOT FOUND IN RDFGraph MAP ->  " + tuple._3);
                            //distinct subject count
                            statistics.setDistinctSubjectCount((RdfUtils.getFromInMemoryModel(
                                    tuple._3,
                                    Main.getShapesStatsPrefixURL() + "distinctSubjectCount",
                                    Main.getShapesModelIRI())).getInt());
                            
                            //distinct object count
                            statistics.setDistinctObjectCount((RdfUtils.getFromInMemoryModel(
                                    tuple._3,
                                    Main.getShapesStatsPrefixURL() + "distinctObjectCount",
                                    Main.getShapesModelIRI())).getInt());
                        }
                        
                    }
                    
                    tempStatList.add(statistics);
                }
            });
            predicatesWithStats.put(predicate, tempStatList);
        });
        return predicatesWithStats;
    }
    
    
    /**
     * This method gets all the Node Shapes with their target classes
     *
     * @return HashMap<String, Tuple3 < String, String, String>>
     */
    public HashMap<String, Tuple3<String, String, String>> getRdfTypePredicatesCandidateShapes() {
        System.out.println("Invoked: getRdfTypePredicatesCandidateShapes()");
        HashMap<String, Tuple3<String, String, String>> hashMap = new HashMap<>();
        
        //get all type defined Node Shapes such that if a predicate of RDF.type have an object of type class, it should be available in the map
        String query = "PREFIX sh: <http://www.w3.org/ns/shacl#>\n" +
                "SELECT distinct ?nodeShape ?nodeShapeTargetClass  WHERE {\n" +
                "    ?nodeShape a sh:NodeShape ;\n" +
                "    sh:targetClass ?nodeShapeTargetClass . \n" +
                "}";
        
        ResultSet x = RdfUtils.runAQueryInMemTDB(query, Main.getShapesModelIRI());
        x.forEachRemaining(querySolution -> {
            // nodeShape -> prefix: shape , nodeShapeTargetClass -> prefix: depends on dataset e.g., yago in Yago dataset
            hashMap.put(querySolution.getResource("nodeShapeTargetClass").getURI(),
                    new Tuple3<>(
                            querySolution.getResource("nodeShapeTargetClass").getURI(),
                            querySolution.getResource("nodeShapeTargetClass").getLocalName(),
                            querySolution.getResource("nodeShape").getURI()));
        });
        return hashMap;
    }
    
    public HashMap<String, Statistics> getRdfTypePredicatesCandidateShapesStats() {
        HashMap<String, Tuple3<String, String, String>> nodeShapes = Main.rdfTypePredicateTargetedNodeShapesMap;
        
        HashMap<String, Statistics> hashMap = new HashMap<>();
        nodeShapes.forEach((s, tuple) -> {
            int subjectCount = (RdfUtils.getFromInMemoryModel(tuple._3, Main.getShapesStatsPrefixURL() + "count", Main.getShapesModelIRI())).getInt();
            Statistics statistics = new Statistics(
                    tuple._3, 1, 1, // default values for min and max count is 1 (Assumption)
                    subjectCount,
                    (RdfUtils.getFromInMemoryModel(tuple._3, Main.getShapesStatsPrefixURL() + "distinctCount", Main.getShapesModelIRI())).getInt(),
                    tuple._2);
            statistics.setSubject(tuple._1);
            statistics.setSubjectCount(subjectCount);
            hashMap.put(s, statistics);
        });
        
        return hashMap;
    }
    
    /**
     * This method queries the SHACL shape graph and get all Shape Properties and then get their statistics and store them in the map
     *
     * @return HashMap<String, List < Integer>>
     */
    public HashMap<String, List<Integer>> getPropertyShapeStatsFromShapeGraph() {
        System.out.println("Invoked: getPropertyShapeStatsFromShapeGraph");
        /* HashMap < Key -> ShapePropertyURI, Value -> List {
        0 -> minCount,
        1 -> maxCount,
        2 -> count,
        3 -> distinctCount,
        4 -> distinctSubjectCount,
        5 -> distinctObjectCount} >
        */
        
        HashMap<String, List<Integer>> shapePropDistMap = new HashMap<>();
        String queryToGetPropertyShapes = "PREFIX sh: <http://www.w3.org/ns/shacl#>\n" +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "PREFIX shape: <http://yago.shacl.io/>\n" +
                "PREFIX stat: <http://yago.shacl.stat.io/>\n" +
                "SELECT distinct ?shapeProperty  WHERE {\n" +
                "   ?shape sh:property ?shapeProperty .\n" +
                "   ?shapeProperty a sh:PropertyShape .\n" +
//                "    ?shapeProperty stat:count ?c .\n" +
                "}";
        //System.out.println(queryToGetPropertyShapes);
        ResultSet x = RdfUtils.runAQueryInMemTDB(queryToGetPropertyShapes, Main.getShapesModelIRI());
        while (x.hasNext()) {
            List<Integer> list = new ArrayList<>();
            String shapePropertyURI = x.next().get("shapeProperty").toString();
            list.add((RdfUtils.getFromInMemoryModel(shapePropertyURI, Main.getShapesStatsPrefixURL() + "minCount", Main.getShapesModelIRI())).getInt());
            list.add((RdfUtils.getFromInMemoryModel(shapePropertyURI, Main.getShapesStatsPrefixURL() + "maxCount", Main.getShapesModelIRI())).getInt());
            list.add((RdfUtils.getFromInMemoryModel(shapePropertyURI, Main.getShapesStatsPrefixURL() + "count", Main.getShapesModelIRI())).getInt());
            list.add((RdfUtils.getFromInMemoryModel(shapePropertyURI, Main.getShapesStatsPrefixURL() + "distinctCount", Main.getShapesModelIRI())).getInt());
            list.add((RdfUtils.getFromInMemoryModel(shapePropertyURI, Main.getShapesStatsPrefixURL() + "distinctSubjectCount", Main.getShapesModelIRI())).getInt());
            list.add((RdfUtils.getFromInMemoryModel(shapePropertyURI, Main.getShapesStatsPrefixURL() + "distinctObjectCount", Main.getShapesModelIRI())).getInt());
            shapePropDistMap.put(shapePropertyURI, list);
        }
        return shapePropDistMap;
    }
    
    
    private String targetClassOfShapeWithLocalName(String query) {
        String result = "";
        ResultSet x = RdfUtils.runAQueryInMemTDB(query, Main.getShapesModelIRI());
        while (x.hasNext()) {
            result = x.next().get("classIRI").toString();
        }
        return result;
    }
    
    
}
