package uk.ac.ox.krr.cardinality.eval;

import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.AlgebraGenerator;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.OpWalker;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.syntax.ElementWalker;
import org.apache.jena.vocabulary.RDF;
import uk.ac.ox.krr.cardinality.queryanswering.model.SPARQLQuery;
import uk.ac.ox.krr.cardinality.queryanswering.reasoner.Reasoner;
import uk.ac.ox.krr.cardinality.queryanswering.util.Prefixes;
import uk.ac.ox.krr.cardinality.util.Dictionary;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.stream.Collectors;

public class JoinOrdering {
    ArrayList<HashSet<Triple>> bgps;
    
    public static ArrayList<Query> tripleQueries = null;
    public static ArrayList<Pair<Triple, Double>> tripleCard = null;
    HashMap<Triple, Double> tripleCardHashMap = null;
    
    HashSet<Triple> triples = null;
    Dictionary dictionary = null;
    Reasoner reasoner = null;
    Prefixes prefixes = null;
    Query query = new Query();
    
    String dir = "";
    String queryName = null;
    
    public JoinOrdering(File queryFile, Reasoner reasoner, Dictionary dictionary, Prefixes prefixes) {
        dir = queryFile.getParent();
        queryName = queryFile.getName();
        this.dictionary = dictionary;
        this.prefixes = prefixes;
        this.reasoner = reasoner;
        
        System.out.println("\nXXX");
        System.out.println("Join Ordering: " + queryFile);
        this.query = QueryFactory.read(queryFile.getPath());
        System.out.println(query.toString());
        
        // get all triples
        bgps = getBGPs(query);
        this.triples = bgps.get(0);
        //System.out.println(triples);
        tripleCard = new ArrayList<>();
        tripleQueries = new ArrayList<>();
        //create queries from each triple and estimate its cardinality
        triples.forEach(triple -> {
            //triple to query or maybe triples to query
            BasicPattern bp = new BasicPattern();
            bp.add(triple);
            Query q = OpAsQuery.asQuery(new OpBGP(bp));
            tripleQueries.add(q);
            
            Evaluator.QueryResult result = new Evaluator.QueryResult();
            try {
                SPARQLQuery newQuery = new SPARQLQuery(q.toString(), dictionary, prefixes);
                result.result = reasoner.answer(newQuery);
                tripleCard.add(new Pair<>(triple, result.result));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        
        joinOrderingAlgo(dictionary, prefixes, reasoner);
        System.out.println("Join ordering completed for this query.");
    }
    
    public void joinOrderingAlgo(Dictionary dictionary, Prefixes prefixes, Reasoner reasoner) {
        List<Pair<Triple, Double>> triplesWithCard = tripleCard.stream().sorted(Comparator.comparing(Pair::getRight)).collect(Collectors.toList());
        System.out.println(triplesWithCard);
        
        tripleCardHashMap = new HashMap<>();
        
        HashMap<Integer, Triple> processed = new HashMap<>();
        HashMap<Integer, Triple> remaining = new HashMap<>();
        
        for (int i = 0, triplesWithCardSize = triplesWithCard.size(); i < triplesWithCardSize; i++) {
            Pair<Triple, Double> doublePair = triplesWithCard.get(i);
            remaining.put(i, doublePair.getLeft());
            tripleCardHashMap.put(doublePair.getLeft(), doublePair.getRight());
        }
        
        Queue<Integer> queue = new LinkedList<>();
        int i = 0;
        double globalCost = 0;
        double sumGlobalCost = 0;
        for (int pointer = 0, triplesWithCardSize = triplesWithCard.size(); pointer < triplesWithCardSize; pointer++) {
            Pair<Triple, Double> tripleCard = triplesWithCard.get(pointer);
            if (pointer == 0) {
                processed.put(0, tripleCard.getLeft());
                globalCost = tripleCard.getRight();
            }
            queue.add(i);
            remaining.remove(i);
            
            //now go for processed vs remaining
            
            Pair<Integer, Pair<Triple, Double>> tripleWithCostAndIndex = null;
            // 1st pair of join-ordering is decided here
            if (pointer == 0) {
                tripleWithCostAndIndex = estimator(i, remaining, processed, globalCost);
                globalCost = tripleWithCostAndIndex.getRight().getRight();
                //Log.logLine("Global Cost: " + globalCost);
            } else {
                Queue<Integer> localQueue = new LinkedList<>(queue);
                //HashMap<Join, Double> joinCostForCurrentQueue = new HashMap<>();
                while (localQueue.size() != 0) {
                    
                    int pointerToPass = localQueue.poll();
                    Pair<Integer, Pair<Triple, Double>> local = estimator(pointerToPass, remaining, processed, globalCost);
                    
                    if (tripleWithCostAndIndex == null) {
                        tripleWithCostAndIndex = local;
                    } else {
                        if (tripleWithCostAndIndex.getRight().getRight() > local.getRight().getRight())
                            tripleWithCostAndIndex = local;
                    }
                }
                assert tripleWithCostAndIndex != null;
                globalCost = tripleWithCostAndIndex.getRight().getRight();
                
            }
            i = tripleWithCostAndIndex.getLeft();
            if (i != 0) {
                processed.put(i, tripleWithCostAndIndex.getRight().getLeft());
                System.out.println(processed.get(i).toString() + "\t -- EC: " + globalCost);
                sumGlobalCost += globalCost;
            }
            
        }
        System.out.println("QERROR," + queryName + "," + globalCost);
        System.out.println(queue);
        BasicPattern bp = new BasicPattern();
        
        for (Integer val : queue) {
            bp.add(processed.get(val));
        }
        Query optimizedQuery = OpAsQuery.asQuery(new OpBGP(bp));
        System.out.println("******** THE QUERY ********");
        System.out.println(optimizedQuery);
        //writeToFile(optimizedQuery.toString(), dir + "/optimized_" + queryName);
        
    }
    
    public Pair<Integer, Pair<Triple, Double>> estimator(int pointer, HashMap<Integer, Triple> remaining, HashMap<Integer, Triple> processed, Double globalCost) {
        Triple tripleA = processed.get(pointer);
        double minCost = -1;
        //String order = "";
        int minKey = 0;
        double tripleACost = globalCost;
        double currentCost = 0;
        boolean joinFlag = false;
        for (Map.Entry<Integer, Triple> entry : remaining.entrySet()) {
            Integer key = entry.getKey();
            Triple tripleB = entry.getValue();
            
            if (tripleA.getSubject().equals(tripleB.getSubject())) {
                //Log.logLine(tripleA.toString() + " SS_JOIN " + tripleB.toString());
                currentCost = estimateJoinCardinality(tripleA, tripleB);
                joinFlag = true;
            }
            if (tripleA.getSubject().toString().equals(tripleB.getObject().toString())) {
                //Log.logLine(tripleA.toString() + " SO_JOIN " + tripleB.toString());
                currentCost = estimateJoinCardinality(tripleA, tripleB);
                
                joinFlag = true;
            }
            if (tripleA.getObject().toString().equals(tripleB.getSubject().toString())) {
                //Log.logLine(tripleA.toString() + " OS_JOIN " + tripleB.toString());
                currentCost = estimateJoinCardinality(tripleA, tripleB);
                
                joinFlag = true;
            }
            if (tripleA.getObject().toString().equals(tripleB.getObject().toString())) {
                //Log.logLine(tripleA.toString() + " OO_JOIN " + tripleB.toString());
                
                //an extra check to handle this kind of case where there will be a cartesian product
                // ?x a ub:GraduateStudent.
                // ?y a ub:GraduateStudent.
                
                if (!tripleA.getSubject().toString().equals(tripleB.getSubject().toString()) &&
                        tripleA.getPredicate().toString().equals(tripleB.getPredicate().toString()) &&
                        tripleA.getPredicate().toString().equals(RDF.type.toString())) {
                    joinFlag = false;
                    //Log.log(" ** CP");
                } else {
                    currentCost = estimateJoinCardinality(tripleA, tripleB);
                    joinFlag = true;
                }
                
            }
            
            // in case no join variable exists between/among the variables of tripleA and tripleB, then its CP
            if (!joinFlag) {
                //Log.log(" CP");
                currentCost = tripleACost * tripleCardHashMap.get(tripleB);
                //Log.log("-- " + tripleACost + " * " + tripleB._2 + " --");
            }
            
            if (minCost == -1 || minCost > currentCost) {
                minCost = currentCost;
                minKey = key;
                //order = pointer + "." + key + " = " + tripleA._1 + " --> " + tripleB._1;
            }
        }
        
        return new Pair<>(minKey, new Pair<>(remaining.get(minKey), minCost));
    }
    
    public double estimateJoinCardinality(Triple tripleA, Triple tripleB) {
        double estimate = 0;
        BasicPattern bp = new BasicPattern();
        bp.add(tripleA);
        bp.add(tripleB);
        Query q = OpAsQuery.asQuery(new OpBGP(bp));
        tripleQueries.add(q);
        
        Evaluator.QueryResult result = new Evaluator.QueryResult();
        try {
            SPARQLQuery newQuery = new SPARQLQuery(q.toString(), dictionary, prefixes);
            result.result = reasoner.answer(newQuery);
            estimate = result.result;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return estimate;
    }
    
    public static ArrayList<HashSet<Triple>> getBGPs(Query query) {
        ArrayList<HashSet<Triple>> bgps = null;
        try {
            ElemVisitor elemVisitor = new ElemVisitor();
            ElementWalker.walk(query.getQueryPattern(), elemVisitor);
            //filterExpression = elemVisitor.getFilterExpression();
            
            Op op = (new AlgebraGenerator()).compile(query);
            BGPVisitor bgpv = new BGPVisitor();
            OpWalker.walk(op, bgpv);
            bgps = bgpv.getBGPs();
        } catch (Exception e) {
            e.printStackTrace();
            //System.exit(1);
        }
        return bgps;
    }
    
    public static void writeToFile(String str, String fileNameAndPath) {
        try {
            FileWriter fileWriter = new FileWriter(new File(fileNameAndPath));
            PrintWriter printWriter = new PrintWriter(fileWriter);
            printWriter.println(str);
            printWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}































