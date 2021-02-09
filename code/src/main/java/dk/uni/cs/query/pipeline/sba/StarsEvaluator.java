package dk.uni.cs.query.pipeline.sba;

import dk.uni.cs.Main;
import dk.uni.cs.query.pipeline.SubQuery;
import dk.uni.cs.shapes.Statistics;
import dk.uni.cs.utils.RdfUtils;
import org.apache.jena.ext.com.google.common.base.Functions;
import org.apache.jena.ext.com.google.common.collect.Lists;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.ResultSetStream;
import org.apache.jena.sparql.engine.join.Join;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;
import org.apache.log4j.Logger;
import org.decimal4j.util.DoubleRounder;

import java.util.*;
import java.util.stream.Collectors;

/*
This approach is called Stars Based Approach in which query is broken into subject-stars and then order of the
different stars is calculated using formulas.
Currently this approach only supports 4 stars.
 */
public class StarsEvaluator {
    final static org.apache.log4j.Logger logger = Logger.getLogger(StarsEvaluator.class);
    
    private HashMap<Triple, List<Statistics>> triplesWithStats;
    private HashMap<Triple, Boolean> isTriplePatternTypeDefined;
    
    public StarsEvaluator() {}
    
    public Vector<SubQuery> getStars(HashSet<Triple> triples) {
        
        Vector<SubQuery> stars = new Vector<>();
        List<Triple> ts = new LinkedList<>(triples);
        HashMap<Node, HashSet<Triple>> bySubject = new HashMap<>();
        HashMap<Node, List<Triple>> bySubjectUpdated = new HashMap<>();
        HashMap<Node, Boolean> rdfTypePresenceChecker = new HashMap<>();
        HashMap<Node, Triple> typeTripleOfStar = new HashMap<>();
        
        for (Triple t : ts) {
            Node s = t.getSubject();
            HashSet<Triple> sts = bySubject.get(s);
            if (sts == null) {
                sts = new HashSet<Triple>();
            }
            sts.add(t);
            bySubject.put(s, sts);
        }
        
        // If there exists rdf type triple in the subject star, it should be the first one.
        for (Map.Entry<Node, HashSet<Triple>> entry : bySubject.entrySet()) {
            boolean flag = false;
            // r: triples of rdf:type
            List<Triple> r = new ArrayList<Triple>();
            //wor: triples without any rdf:type triple
            List<Triple> wor = new ArrayList<Triple>();
            Node n = entry.getKey();
            HashSet<Triple> v = entry.getValue();
            for (Triple triple : v) {
                if (isRdfTypeTriple(triple) && !triple.getObject().isVariable()) {
                    flag = true;
                    r.add(triple);
                    typeTripleOfStar.put(n,triple);
                } else {
                    wor.add(triple);
                }
            }
            if (r.size() > 1) {
                System.out.println("WARNING: Query star is of two RDF types.");
            }
            r.addAll(wor);
            bySubjectUpdated.put(n, r);
            if (flag) {
                rdfTypePresenceChecker.put(n, true);
            } else {
                rdfTypePresenceChecker.put(n, false);
            }
        }
        
        for (Node s : bySubjectUpdated.keySet()) {
            List<Triple> starSubj = bySubjectUpdated.get(s);
            SubQuery sq = new SubQuery(starSubj);
            sq.setSubjectStar();
            sq.setRdfTypeFlag(rdfTypePresenceChecker.get(s));
            if (rdfTypePresenceChecker.get(s)) sq.setTypeDefinedTriple(typeTripleOfStar.get(s));
            stars.add(sq);
        }
        return stars;
    }
    
    public Vector<SubQuery> getStars(HashSet<Triple> triples, HashMap<Triple, List<Statistics>> relevantShapes) {
        
        Vector<SubQuery> stars = new Vector<SubQuery>();
        List<Triple> ts = new LinkedList<Triple>(triples);
        HashMap<Node, HashSet<Triple>> bySubject = new HashMap<Node, HashSet<Triple>>();
        HashMap<Node, List<Triple>> bySubjectUpdated = new HashMap<Node, List<Triple>>();
        HashMap<Node, Boolean> rdfTypePresenceChecker = new HashMap<Node, Boolean>();
        HashMap<Node, Triple> typeTripleOfStar = new HashMap<>();

        /*for (Triple t : ts) {
            Node o = t.getObject();
            HashSet<Triple> ots = byObject.get(o);
            if (ots == null) {
                ots = new HashSet<Triple>();
            }
            ots.add(t);
            byObject.put(o, ots);
        }*/
        for (Triple t : ts) {
            Node s = t.getSubject();
            HashSet<Triple> sts = bySubject.get(s);
            if (sts == null) {
                sts = new HashSet<Triple>();
            }
            sts.add(t);
            bySubject.put(s, sts);
        }
        // If there exists rdf type triple in the subject star, it should be the first one.
        
        for (Map.Entry<Node, HashSet<Triple>> entry : bySubject.entrySet()) {
            boolean flag = false;
            // r: triples of rdf:type
            List<Triple> r = new ArrayList<Triple>();
            //wor: triples without any rdf:type triple
            List<Triple> wor = new ArrayList<Triple>();
            Node n = entry.getKey();
            HashSet<Triple> v = entry.getValue();
            for (Triple triple : v) {
                if (isRdfTypeTriple(triple) && !triple.getObject().isVariable()) {
                    flag = true;
                    r.add(triple);
                    typeTripleOfStar.put(n,triple);
                } else {
                    wor.add(triple);
                }
            }
            r.addAll(wor);
            bySubjectUpdated.put(n, r);
            if (flag) {
                rdfTypePresenceChecker.put(n, true);
            } else {
                rdfTypePresenceChecker.put(n, false);
            }
        }
        
        for (Node s : bySubjectUpdated.keySet()) {
            List<Triple> starSubj = bySubjectUpdated.get(s);
            SubQuery sq = new SubQuery(starSubj);
            sq.setSubjectStar();
            sq.setRdfTypeFlag(rdfTypePresenceChecker.get(s));
            HashMap<Triple, List<Statistics>> shapesOfStar = new HashMap<>();
            if (rdfTypePresenceChecker.get(s)) sq.setTypeDefinedTriple(typeTripleOfStar.get(s));
            //sq.getPredicates();
            //Identifying triples with shapes for the relevant star query. Every star should contain the shapes stat for its relevant triples
            starSubj.forEach(triple -> {
                if (relevantShapes.containsKey(triple)) {
                    List<Statistics> x = relevantShapes.get(triple);
                    shapesOfStar.put(triple, x);
                }
                
            });
            sq.setShapes(shapesOfStar);
            stars.add(sq);
        }

       /* for (Node s : bySubject.keySet()) {
            HashSet<Triple> starSubj = bySubject.get(s);
            SubQuery sq = new SubQuery(starSubj);
            sq.setSubjectStar();

            HashMap<Triple, List<Statistics>> shapesOfStar = new HashMap<>();

            //sq.getPredicates();
            //Identifying triples with shapes for the relevant star query. Every star should contain the shapes stat for its relevant triples
            starSubj.forEach(triple -> {
                if (relevantShapes.containsKey(triple)) {
                    List<Statistics> x = relevantShapes.get(triple);
                    shapesOfStar.put(triple, x);
                }

            });
            sq.setShapes(shapesOfStar);
            stars.add(sq);
        }*/
        return stars;
    }
    
    // In this method we iterate over the subqueries to extract star patterns with their statistics (coming from the shapes)
    public List<Star> iterateSubQueriesToExtractStarsWithStats(Vector<SubQuery> stars, Query query) {
        List<Star> starList = new ArrayList<>();
        RanksCalculator ranksCalculator = new RanksCalculator(this);
        for (SubQuery sq : stars) {
            //System.out.println("Parsing SubQuery " + sq.toString());
            List<Triple> rdfTypeTriplesInStar = new ArrayList<>();
            List<TripleWithStats> tripleWithStatsArrayList = new ArrayList<>();
            double rdfTypeTripleRank = 0;
            
            // Go through each triple of star and compute ranking
            for (Triple triple : sq.getTriples()) {
                //System.out.println(triple);
                double rank = 0, dsc = 0, doc = 0;
                
                // ************************ CASE A *******************************
                if (!triple.getObject().isVariable()) {
                    rank = ranksCalculator.getRankWhenObjIsNotVar(sq, triple, rank);
                }
                // ************************  CASE B *******************************
                if (triple.getObject().isVariable()) {
                    rank = ranksCalculator.getRankWhenObjIsVar(sq, triple, rank);
                }
                
                //TODO need to handle when there exists more than one rdf type triples in the star
                if (sq.isTypeDefined && triple.getPredicate().toString().equals(RDF.type.toString())) {
                    rdfTypeTripleRank = rank;
                }
                if (sq.isTypeDefined) {
                    dsc = rdfTypeTripleRank;
                } else {
                    dsc = ranksCalculator.getCountDistinctSubjects();
                }
                //dsc = ranksCalculator.getCountDistinctSubjects();
                doc = ranksCalculator.getCountDistinctObjects();
                
                //System.out.println("dsc: " + dsc + "  doc: " + doc);
                tripleWithStatsArrayList.add(new TripleWithStats(DoubleRounder.round(rank, 5), dsc, doc, triple));
                
                if (triple.getPredicate().toString().equals(RDF.type.toString())) {
                    rdfTypeTriplesInStar.add(triple);
                }
            }
            tripleWithStatsArrayList = tripleWithStatsArrayList.stream().sorted(Comparator.comparing(TripleWithStats::getCardinality)).collect(Collectors.toList());
            
            double estimatedStarCardinality = getEstimatedStarCardinality(tripleWithStatsArrayList);
            //System.out.println(estimatedStarCardinality + " - " + tripleWithStatsArrayList.toString());
            boolean orphanityFlag = false;
            if (rdfTypeTriplesInStar.size() > 0 && tripleWithStatsArrayList.size() > 1) {
                orphanityFlag = checkOrphanTripleWithinStar(rdfTypeTriplesInStar, tripleWithStatsArrayList);
            }
            
            if (orphanityFlag) {
                estimatedStarCardinality = 0;
            }
            starList.add(new Star(estimatedStarCardinality, tripleWithStatsArrayList));
        }
        return starList;
    }
    
    /**
     * Orphan triple is a triple which doesn't belong to that specific defined class/shape.
     * E.g., in query SELECT * WHERE { ?X ub:undergraduateDegreeFrom ?Y . ?X rdf:type ub:UndergraduateStudent . ?Y rdf:type ub:University . ?X ub:memberOf ?Z . ?Z ub:subOrganizationOf ?Y . ?Z rdf:type ub:Department .  }
     * triple ?X ub:undergraduateDegreeFrom ?Y  is orphan as it doesn't belong to the defined type ub:UndergraduateStudent
     * This will impact the overall star cardinality which leads to zero number of returned rows.
     */
    private boolean checkOrphanTripleWithinStar(List<Triple> rdfTypeTriplesInStar, List<TripleWithStats> tripleWithStatsArrayList) {
        HashSet<String> propertiesHashSet = new HashSet<>();
        boolean flag = false;
        // What if the type is not defined?, e.g., ?v rdf:type x is a triple where the rdf type is not defined.
        
        if (rdfTypeTriplesInStar.size() == 1) {
            
            for (Triple triple : rdfTypeTriplesInStar) {
                if (!triple.getObject().isURI())
                    return flag;
                String nodeShape = Main.getShapesPrefixURL() + triple.getObject().getLocalName() + "Shape";
                List<Resource> result = RdfUtils.getObjectsFromModel(nodeShape, "http://www.w3.org/ns/shacl#property", Main.getShapesModelIRI());
                //System.out.println(result);
                for (Resource resource : result) {
                    propertiesHashSet.add(RdfUtils.getObjectsFromModel(resource.toString(), "http://www.w3.org/ns/shacl#path", Main.getShapesModelIRI()).get(0).toString());
                }
                propertiesHashSet.add(triple.getPredicate().toString());
            }
            //for (String p : propertiesHashSet) { System.out.println(p); }
            
            for (TripleWithStats tripleWithStats : tripleWithStatsArrayList) {
                if (!propertiesHashSet.contains(tripleWithStats.triple.getPredicate().toString()) && !tripleWithStats.triple.getPredicate().toString().equals(OWL.sameAs.toString())) {
                    logger.info("Orphan triple: " + tripleWithStats.triple);
                    flag = true;
                }
            }
        } else {
            //TODO: what if more than one rdf:type is defined? need to handle this case....
        }
        
        return flag;
        
    }
    
    public static double getEstimatedStarCardinality(List<TripleWithStats> tripleWithStatsArrayList) {
        double estimatedStarCardinality;
        double nom = 1D;
        double maxDistinctSubjectCount = 1D;
        maxDistinctSubjectCount = tripleWithStatsArrayList.stream().mapToDouble(TripleWithStats::getDistinctSubjects).max().getAsDouble();
        
        for (TripleWithStats tripleWithStats : tripleWithStatsArrayList) {
            if (tripleWithStats.cardinality != 0) {
                nom = nom * tripleWithStats.cardinality;
            }
        }
        
        if (tripleWithStatsArrayList.size() == 1) {
            estimatedStarCardinality = nom;
        } else {
            estimatedStarCardinality = nom / maxDistinctSubjectCount;
        }
        return estimatedStarCardinality;
    }
    
    private boolean isJoinVariable(boolean flag, Triple lastTripleOfThePrevStar, Triple currentTripleInTheLoop) {
        if (lastTripleOfThePrevStar.objectMatches(currentTripleInTheLoop.getObject())) {
            //System.out.println("Object Object Match Found");
            flag = true;
        }
        
        if (lastTripleOfThePrevStar.objectMatches(currentTripleInTheLoop.getSubject())) {
            //System.out.println("obj Subj Match Found");
            flag = true;
        }
        
        if (lastTripleOfThePrevStar.subjectMatches(currentTripleInTheLoop.getObject())) {
            //System.out.println("Subj Obj Match Found");
            flag = true;
        }
        return flag;
    }
    
    private double estimateIntermediateResultSize(double starACardinality, double v1, double starBCardinality, double v2) {
        double nom = starACardinality * starBCardinality;
        double den = Math.max(v1, v2);
        return nom / den;
    }
    
    public boolean isRdfTypeTriple(Triple triple) {
        return triple.getPredicate().toString().equals(RDF.type.toString());
    }
    
    //Getter Methods
    public HashMap<Triple, List<Statistics>> getTriplesWithStats() {
        return triplesWithStats;
    }
    
    public HashMap<Triple, Boolean> getIsTriplePatternTypeDefined() { return isTriplePatternTypeDefined; }
    
    //Setter Methods
    public void setTriplesWithStats(HashMap<Triple, List<Statistics>> triplesWithStats) { this.triplesWithStats = triplesWithStats; }
    
    public void setIsTriplePatternTypeDefined(HashMap<Triple, Boolean> isTriplePatternTypeDefined) { this.isTriplePatternTypeDefined = isTriplePatternTypeDefined; }
    
    //Not required anymore
    private void customJoinExecutor(List<Op> opList, Query query) {
        //System.out.println(opList);
        //System.out.println(opList.size());
        
        Dataset ds = RdfUtils.getTDBDataset();
        Model gm = ds.getNamedModel(Main.getRdfModelIRI());
        ds.begin(ReadWrite.READ);
        
        QueryIterator queryIterator = null;
        
        queryIterator = Join.nestedLoopJoin(Algebra.exec(opList.get(0), gm.getGraph()), Algebra.exec(opList.get(1), gm.getGraph()), null);
        queryIterator = Join.nestedLoopJoin(queryIterator, Algebra.exec(opList.get(2), gm.getGraph()), null);
        queryIterator = Join.nestedLoopJoin(queryIterator, Algebra.exec(opList.get(3), gm.getGraph()), null);

 /*       if (opList.size() == 1) {
            System.out.println(Algebra.optimize(opList.get(0)));
            queryIterator = Algebra.exec(opList.get(0), gm.getGraph());
        } else {

            if (opList.size() >= 2) {
                System.out.println(Algebra.optimize(opList.get(0)));
                System.out.println(Algebra.optimize(opList.get(1)));

                queryIterator = Join.nestedLoopJoin(Algebra.exec(opList.get(0), gm.getGraph()), Algebra.exec(opList.get(1), gm.getGraph()), null);

                for (int index = 2; index < opList.size(); index++) {
                    System.out.println(Algebra.optimize(opList.get(index)));
                    queryIterator = Join.nestedLoopJoin(queryIterator, Algebra.exec(opList.get(index), gm.getGraph()), null);
                }
            }
        }*/
        
        ResultSet resultSet = new ResultSetStream(Lists.transform(query.getProjectVars(), Functions.toStringFunction()), gm, queryIterator);
        System.out.println("Result");
        System.out.println(ResultSetFormatter.consume(resultSet));
        
        ds.end();
        ds.close();
    }
    
    private BasicPattern getOrderedTriplesBP(List<Star> starList) {
        // Number of Subject Stars = Size of the List of stars;
        // For each star we need to estimate the intermediate result size of join with the other star
        HashMap<String, Double> joinPairCost = new HashMap<>();
        
        int starsCount = starList.size();
        int[][] index = new int[starsCount][starsCount];
        int i = 0;
        logger.info(starList.toString());
        //System.out.println(starList.toString());
        
        //iterate over list of stars
        //outer loop
        for (Star starA : starList) {
            int j = 0;
            for (Star starB : starList) {
                if (i != j) {
                    if (index[j][i] == 0) {
                        //main logic here
                        
                        //System.out.println("\nSTAR [" + (i + 1) + "][" + (j + 1) + "]  -->  ");
                        boolean joinFlag = false;
                        double cost = 0;
                        // StarA JOIN StarB
                        for (TripleWithStats starATriple : starA.tripleWithStats) {
                            for (TripleWithStats starBTriple : starB.tripleWithStats) {
                                //if(starATriple.triple.getSubject().equals(starBTriple.triple.getSubject())) {
                                //    System.out.println(starATriple.triple.toString() + " SS_JOIN " + starBTriple.triple.toString());
                                //}
                                
                                if (starATriple.triple.getSubject().toString().equals(starBTriple.triple.getObject().toString())) {
                                    //System.out.println(starATriple.triple.toString() + " SO_JOIN " + starBTriple.triple.toString());
                                    cost = estimateIntermediateResultSize(starA.starCardinality, starATriple.distinctSubjects, starB.starCardinality, starBTriple.distinctObjects);
                                    
                                    joinFlag = true;
                                }
                                
                                if (starATriple.triple.getObject().toString().equals(starBTriple.triple.getSubject().toString())) {
                                    //System.out.println(starATriple.triple.toString() + " OS_JOIN " + starBTriple.triple.toString());
                                    cost = estimateIntermediateResultSize(starA.starCardinality, starATriple.distinctObjects, starB.starCardinality, starBTriple.distinctSubjects);
                                    
                                    joinFlag = true;
                                }
                                
                                if (starATriple.triple.getObject().toString().equals(starBTriple.triple.getObject().toString())) {
                                    //System.out.println(starATriple.triple.toString() + " OO_JOIN " + starBTriple.triple.toString());
                                    cost = estimateIntermediateResultSize(starA.starCardinality, starATriple.distinctObjects, starB.starCardinality, starBTriple.distinctObjects);
                                    
                                    joinFlag = true;
                                }
                            }
                            
                        }
                        if (!joinFlag) {
                            cost = starA.starCardinality * starB.starCardinality;
                        }
                        
                        //System.out.println((i + 1) + "." + (j + 1) + " -> " + starA.starCardinality + " -- " + starB.starCardinality);
                        joinPairCost.put((i + 1) + "." + (j + 1), cost);
                        index[i][j] = -1;
                    }
                }
                j++;
            }
            i++;
        }
        
        System.out.println(joinPairCost);
        
        String order = new CostMapper().decider(joinPairCost, starList);
        System.out.println(order);
        
        BasicPattern bp = new BasicPattern();
        List<Op> opList = new ArrayList<>();
        
        if (starList.size() > 2) {
            // start
            List<Star> orderedStars = new ArrayList<>();
            for (String orderIndex : order.split("\\.")) {
                orderedStars.add(starList.get(Integer.parseInt(orderIndex) - 1));
            }
            
            for (int n = 0; n < orderedStars.size(); n++) {
                //System.out.println(orderedStars.get(n).triples);
                if (n > 0) {
                    
                    //boolean retain = orderedStars.get(n).starTriplesVars.retainAll(orderedStars.get(n-1).starTriplesVars);
                    //System.out.println(orderedStars.get(n).starTriplesVars.stream().filter(orderedStars.get(n - 1).starTriplesVars::contains).count());
                    
                    if (orderedStars.get(n).starTriplesVars.stream().noneMatch(orderedStars.get(n - 1).starTriplesVars::contains)) {
                        if (n + 1 < orderedStars.size()) {
                            Collections.swap(orderedStars, n, n + 1);
                            System.out.println("Swapped");
                            //System.out.println(orderedStars.get(n).starTriplesVars.stream().filter(orderedStars.get(n - 1).starTriplesVars::contains).count());
                        }
                    }
                    //System.out.println(orderedStars.get(n-1).starTriplesVars);
                    
                }
            }
            //System.out.println(orderedStars.toString());
            /*for (Star star : orderedStars) {
                for (TripleWithStats triples : star.triples) {
                    bp.add(triples.triple);
                }
            }*/
            //end
            
            
            int starNumber = 0;
            for (Star star : orderedStars) {
                BasicPattern basicPattern = new BasicPattern();
                //Get the list of triples of the current star
                List<TripleWithStats> ListOfTriplesOfTheCurrentStar = star.tripleWithStats;
                
                //System.out.println("star: " + starNumber + " order: " + indexNumber + " Triples: " + ListOfTriplesOfTheCurrentStar.size() + " Current Size of BP: " + bp.size());
                
                
                boolean flag = false;
                // loop through all the triples and add them to bp
                for (int loopIndex = 0; loopIndex < ListOfTriplesOfTheCurrentStar.size(); loopIndex++) {
                    
                    TripleWithStats cTriple = ListOfTriplesOfTheCurrentStar.get(loopIndex);
                    
                    if (starNumber >= 1 && !flag) {
                        //check the last triple added in the bp, get its variables, try to match them with the current triple in
                        //the loop, if it matches, go ahead, otherwise reserve it and try to match with the other triple
                        
                        Triple lastTripleOfThePrevStar = bp.get(bp.size() - 1);
                        Triple currentTripleInTheLoop = cTriple.triple;
                        
                        //System.out.println(lastTripleOfThePrevStar + " --> " + currentTripleInTheLoop);
                        
                        flag = isJoinVariable(flag, lastTripleOfThePrevStar, currentTripleInTheLoop);
                        
                        if (flag && ListOfTriplesOfTheCurrentStar.size() > 1) {
                            //There is no join variable, now we have to check with the next triple
                            //System.out.println("Swapping ...");
                            Collections.swap(ListOfTriplesOfTheCurrentStar, 0, loopIndex);
                        }
                    }
                    
                }
                
                for (TripleWithStats cTriple : ListOfTriplesOfTheCurrentStar) {
                    bp.add(cTriple.triple);
                    basicPattern.add(cTriple.triple);
                }
                
                Op algebraOpBasicPatter = new OpBGP(basicPattern);
                opList.add(algebraOpBasicPatter);
                starNumber++;
            }
        } else {
            //for stars less than 2
            for (Star star : starList) {
                for (TripleWithStats triples : star.tripleWithStats) {
                    bp.add(triples.triple);
                }
            }
        }
        //customJoinExecutor(opList, query);
        return bp;
    }
}


/*    StarsEvaluator(String query) {
        this.posedQuery = query;
    }
    
    Op useShapesToGenerateOptimizedAlgebra() {
        Op algebraOp = null;
        Query query = QueryFactory.create(posedQuery);
        projectedVariables = query.getProjectVars();
        ArrayList<HashSet<Triple>> bgps = QueryUtils.getBGPs(query);
        //for every Basic Graph Pattern
        for (HashSet<Triple> triples : bgps) {
            
            HashMap<Triple, List<Tuple3<String, String, String>>> triplesToShapesMapping = new ShapesDetector(triples, true).getCandidateShapesOfTriples();
            
            HashMap<Triple, List<Statistics>> triplesWithStats = new HashMap<>();
            
            // Get statistics
            queryUtils.getStatisticsFromShapes(triplesToShapesMapping, triplesWithStats);
            
            // Get Stars
            Vector<SubQuery> stars = getStars(triples, triplesWithStats);
            
            //Iterate over stars to estimate the cardinality
            List<Star> starsWithStats = iterateSubQueriesToExtractStarsWithStats(stars, query);
            
            algebraOp = queryUtils.makeQueryAlgebra(query, getOrderedTriplesBP(starsWithStats), projectedVariables);
            
            System.out.println(algebraOp);
        }
        return algebraOp;
    }*/