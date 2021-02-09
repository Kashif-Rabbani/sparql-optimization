package dk.uni.cs.shapes;

import dk.uni.cs.Main;
import dk.uni.cs.query.pipeline.sba.StarsEvaluator;
import dk.uni.cs.query.pipeline.SubQuery;
import dk.uni.cs.utils.RdfUtils;
import dk.uni.cs.utils.Tuple3;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.ResultSet;
import org.apache.jena.vocabulary.RDF;

import java.util.*;
import java.util.stream.Collectors;

/**
 * This class is to detect the candidate shapes for the given BGPs of the input query
 */
public class ShapesDetector {
    private final String shapesURLPrefix = Main.getShapesPrefixURL();
    private final String model = Main.getRdfModelIRI();
    private ArrayList<HashSet<Triple>> bgps;
    private HashMap<Triple, List<Tuple3<String, String, String>>> candidateShapesOfTriples;
    private final Set<String> shapesHashSet = new HashSet<String>();
    HashMap<Triple, List<Statistics>> triplesWithStats = new HashMap<>();
    
    StarsEvaluator starsEvaluator = new StarsEvaluator();
    List<String> rdfTypeValues = new ArrayList<>();
    Vector<SubQuery> queryStars;
    HashMap<Triple, Boolean> isTriplePatternTypeDefined = new HashMap<>();
    
    public ShapesDetector() {}
    
    public ShapesDetector(ArrayList<HashSet<Triple>> bgps) {
        this.bgps = bgps;
        findShapes();
    }
    
    private void findShapes() {
        candidateShapesOfTriples = new HashMap<Triple, List<Tuple3<String, String, String>>>();
        for (HashSet<Triple> triples : bgps) {
            findCandidateShapesOfTPsViaShapeApproach(triples);
        }
    }
    
    public ShapesDetector(HashSet<Triple> triples, Boolean shapeApproachFlag) {
        candidateShapesOfTriples = new HashMap<>();
        if (shapeApproachFlag) findCandidateShapesOfTPsViaShapeApproach(triples);
        else getCandidateShapesOfTriplesForBaselineApproach(triples);
    }
    
    private void findCandidateShapesOfTPsViaShapeApproach(HashSet<Triple> triples) {
        queryStars = starsEvaluator.getStars(triples);
        
        //iterate over the triples of the Star to get the type defined values e.g., tp1: ?s a ub:GraduateStudent, tp2: ?x a ub:Professor.
        // the resultant list will contain i) ub:GraduateStudent and ii) ub:Professor
        // also, prepare a HashMap for each triple to its boolean value showing if this triple belongs to a star whose type is defined.
        queryStars.forEach(star -> {
            if (star.isTypeDefined()) {
                star.getTriples().forEach(triple -> {
                    isTriplePatternTypeDefined.put(triple, true);
                    if (starsEvaluator.isRdfTypeTriple(triple)) rdfTypeValues.add(triple.getObject().toString());
                });
            } else {
                star.getTriples().forEach(triple -> {
                    isTriplePatternTypeDefined.put(triple, false);
                });
            }
        });
        
        //Iterate over each Star's triples to get its candidate Shapes
        //Note that it will also return the MetaData Shape for each triple as well.
        queryStars.forEach(star -> star.getTriples().forEach(triple -> {
            {
                //System.out.println("Triple: " + triple.getSubject() + " - " + triple.getPredicate() + " - " + triple.getObject());
                List<Tuple3<String, String, String>> queryRelatives = new ArrayList<>();
                
                if (triple.getSubject().isURI()) {
                    if (this.uriIsClass(triple.getSubject().getURI())) {
                        queryRelatives.add(new Tuple3<>(
                                triple.getSubject().getURI(),
                                triple.getSubject().getLocalName(),
                                shapesURLPrefix + triple.getSubject().getLocalName() + "Shape")
                        );
                        shapesHashSet.add(shapesURLPrefix + triple.getSubject().getLocalName() + "Shape");
                    }
                    
                    List<Tuple3<String, String, String>> finalQueryRelatives = queryRelatives;
                    this.uriIsInstanceOfClass(triple.getSubject().getURI()).forEachRemaining(t -> {
                        finalQueryRelatives.add(new Tuple3<>(
                                t.get("type").toString(),
                                t.getResource("type").getLocalName(),
                                shapesURLPrefix + t.getResource("type").getLocalName() + "Shape")
                        );
                        shapesHashSet.add(shapesURLPrefix + t.getResource("type").getLocalName() + "Shape");
                    });
                }
                
                if (triple.getPredicate().isURI()) {
                    if (!triple.getPredicate().getURI().equals(RDF.type.getURI())) {
                        //System.out.println(triple);
                        if (Main.predicateShapeDistMap.containsKey(triple.getPredicate().toString())) {
                            //here if the type is defined, then I should get the Node Shape Property of that shape only
                            queryRelatives = Main.predicateShapeDistMap.get(triple.getPredicate().toString());
                            if (star.isTypeDefined()) {
                                queryRelatives = queryRelatives.stream().filter(tuple3 -> {
                                    boolean flag = false;
                                    if (star.getTypeDefinedTriple().getObject().toString().equals(tuple3._1) || tuple3._2.equals("RDFGraph"))
                                        flag = true;
                                    return flag;
                                }).collect(Collectors.toList());
                            }
                            
                        }
                    }
                }
                
                if (triple.getObject().isURI()) {
                    if (this.uriIsClass(triple.getObject().getURI())) {
                        queryRelatives.add(new Tuple3<>(
                                triple.getObject().getURI(),
                                triple.getObject().getLocalName(),
                                shapesURLPrefix + triple.getObject().getLocalName() + "Shape")
                        );
                        shapesHashSet.add(shapesURLPrefix + triple.getObject().getLocalName() + "Shape");
                    }
                }
                candidateShapesOfTriples.put(triple, queryRelatives);
            }
        }));
        
        //Use the triples to Candidate Shapes Mappings to extract the statistics
        statsExtractor();
    }
    
    private void getCandidateShapesOfTriplesForBaselineApproach(HashSet<Triple> triples) {
        HashSet<Triple> ts = new HashSet<Triple>(triples);
        
        ts.forEach(triple -> {
            //System.out.println("Triple: " + triple.getSubject() + " - " + triple.getPredicate() + " - " + triple.getObject());
            List<Tuple3<String, String, String>> queryRelatives = new ArrayList<>();
            
            if (triple.getSubject().isURI()) {
                if (this.uriIsClass(triple.getSubject().getURI())) {
                    queryRelatives.add(new Tuple3<>(
                            triple.getSubject().getURI(),
                            triple.getSubject().getLocalName(),
                            shapesURLPrefix + triple.getSubject().getLocalName() + "Shape")
                    );
                    shapesHashSet.add(shapesURLPrefix + triple.getSubject().getLocalName() + "Shape");
                }
                
                List<Tuple3<String, String, String>> finalQueryRelatives = queryRelatives;
                this.uriIsInstanceOfClass(triple.getSubject().getURI()).forEachRemaining(t -> {
                    finalQueryRelatives.add(new Tuple3<>(
                            t.get("type").toString(),
                            t.getResource("type").getLocalName(),
                            shapesURLPrefix + t.getResource("type").getLocalName() + "Shape")
                    );
                    shapesHashSet.add(shapesURLPrefix + t.getResource("type").getLocalName() + "Shape");
                });
                
            }
            
            if (triple.getPredicate().isURI()) {
                if (!triple.getPredicate().getURI().equals(RDF.type.getURI())) {
                    //System.out.println(triple);
                    if (Main.predicateShapeDistMap.containsKey(triple.getPredicate().toString())) {
                        queryRelatives = Main.predicateShapeDistMap.get(triple.getPredicate().toString());
                        //System.out.println(queryRelatives.size());
                    }
                }
            }
            
            if (triple.getObject().isURI()) {
                if (this.uriIsClass(triple.getObject().getURI())) {
                    queryRelatives.add(new Tuple3<>(
                            triple.getObject().getURI(),
                            triple.getObject().getLocalName(),
                            shapesURLPrefix + triple.getObject().getLocalName() + "Shape")
                    );
                    shapesHashSet.add(shapesURLPrefix + triple.getObject().getLocalName() + "Shape");
                }
            }
            
            candidateShapesOfTriples.put(triple, queryRelatives);
        });
        
        statsExtractor();
    }
    
    private void statsExtractor() {
        candidateShapesOfTriples.forEach((triple, list) -> {
            List<Statistics> tempStatList = new ArrayList<Statistics>();
            List<Statistics> stats = new ArrayList<Statistics>();
            //System.out.println("Triple: " + triple + " : " + list.size());
            List<String> listShapesIRIs = new ArrayList<>();
            boolean flag = false;
            
            if (isTriplePatternTypeDefined.containsKey(triple)) {
                flag = true;
                list.forEach(tuple3 -> {
                    listShapesIRIs.add(tuple3._3);
                });
            }
            //Marker :- In Memory Mappings Resolver
            if (triple.getPredicate().toString().equals(RDF.type.toString()) && !triple.getObject().isVariable()) {
                if (Main.rdfTypePredicateTargetedNodeShapesMap.containsKey(triple.getObject().toString())) {
                    //System.out.println("CommonComponents - Contains RDF.type predicate");
                    //Main.rdfTypePredicateTargetedNodeShapesMap.get(triple.getObject().toString());
                    tempStatList.add(Main.rdfTypePredicateTargetedNodeShapesStatsMap.get(triple.getObject().toString()));
                }
            }
            if (Main.predicatesToShapesStatsMapping.containsKey(triple.getPredicate().toString())) {
                //System.out.println("CommonComponents - Contains predicate");
                stats = Main.predicatesToShapesStatsMapping.get(triple.getPredicate().toString());
                
                if (flag) {
                    stats = stats.stream().filter(statistics -> {
                        boolean exists = false;
                        if (listShapesIRIs.contains(statistics.getShapeIRI()))
                            exists = true;
                        return exists;
                    }).collect(Collectors.toList());
                }
                
                
                tempStatList.addAll(stats);
            }
            
            triplesWithStats.put(triple, tempStatList);
        });
    }
    
    private boolean uriIsClass(String uri) {
        long a = System.currentTimeMillis();
        //ASK { ?s a ?type. FILTER EXISTS {?s a mo:MusicalWork .}}
        //SELECT DISTINCT ?type WHERE { ?s a ?type. FILTER EXISTS {?s a mo:MusicalWork .}}
        //System.out.println("Find out if URI is of Class");
        String q = "ASK { FILTER EXISTS {?s a <" + uri + "> . }}";
        //System.out.println(q);
        //System.out.println("In total uriIsClass method took " + (System.currentTimeMillis() - a) + " MS");
        return RdfUtils.runAskQuery(q, model);
        
    }
    
    private ResultSet uriIsInstanceOfClass(String uri) {
        long a = System.currentTimeMillis();
        //System.out.println("URI is an instance of some class");
        String q = "SELECT ?type WHERE { <" + uri + "> a ?type . }";
        //System.out.println(q);
        //System.out.println("In total uriIsInstanceOfClass method took " + (System.currentTimeMillis() - a) + " MS");
        return RdfUtils.runAQuery(q, model);
        
    }
    
    public HashMap<Triple, List<Tuple3<String, String, String>>> getCandidateShapesOfTriples() {
        return candidateShapesOfTriples;
    }
    
    public HashMap<Triple, List<Statistics>> getTriplesWithStats() {
        return triplesWithStats;
    }
    
    private ResultSet uriClassBelongings(String uri) {
        //SELECT DISTINCT ?type { ?s <URI> e.g. dc:title ?x . ?s a ?type .}
        //System.out.println("Find out class belongings of a URI");
        String q = "SELECT DISTINCT ?type { ?s <" + uri + "> ?x . ?s a ?type . }";
        return RdfUtils.runAQuery(q, model);
    }
    
    private String targetClassOfShapeWithLocalName(String query) {
        String result = "";
        ResultSet x = RdfUtils.runAQueryInMemTDB(query, Main.getShapesModelIRI());
        while (x.hasNext()) {
            result = x.next().get("classIRI").toString();
        }
        return result;
    }
    
    public Set<String> getShapesHashSet() {
        return shapesHashSet;
    }
    
    public HashMap<Triple, Boolean> getIsTriplePatternTypeDefined() {
        return isTriplePatternTypeDefined;
    }
}
