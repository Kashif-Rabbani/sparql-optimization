package dk.uni.cs.query.pipeline.sba;

import dk.uni.cs.query.pipeline.CostComputer;
import dk.uni.cs.query.pipeline.QueryUtils;
import dk.uni.cs.query.pipeline.SubQuery;
import dk.uni.cs.shapes.Statistics;
import dk.uni.cs.utils.Tuple3;
import dk.uni.cs.utils.Tuple4;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;

import java.util.*;
import java.util.stream.Collectors;

public class SBA {
    QueryUtils queryUtils = new QueryUtils();
    
    double globalPlanCost;
    Query query;
    List<Tuple4<Triple, Double, Double, Double>> tripleWithStats = new ArrayList<>();
    List<Var> projectedVariables;
    Star firstStar = null;
    List<Star> stars = null;
    
    public SBA(Query query, HashSet<Triple> triples, HashMap<Triple, List<Statistics>> triplesWithStats, HashMap<Triple, Boolean> isTriplePatternTypeDefined) {
        this.query = query;
        this.projectedVariables = query.getProjectVars();
        
        StarsEvaluator starsEvaluator = new StarsEvaluator();
        List<TripleWithStats> tws = new ArrayList<>();
        
        //Extract Stars with Statistics
        starsEvaluator.setTriplesWithStats(triplesWithStats);
        starsEvaluator.setIsTriplePatternTypeDefined(isTriplePatternTypeDefined);
        
        Vector<SubQuery> subQueries = starsEvaluator.getStars(triples, triplesWithStats);
        stars = starsEvaluator.iterateSubQueriesToExtractStarsWithStats(subQueries, query);
        stars.stream().map(star -> star.tripleWithStats).forEach(tws::addAll);
        tws.forEach(t -> tripleWithStats.add(new Tuple4<>(t.triple, t.cardinality, t.distinctSubjects, t.distinctObjects)));
        firstStar = stars.stream().sorted(Comparator.comparing(Star::getValueOfLeastCard)).collect(Collectors.toList()).get(0);
    }
    
    
    public Op opSBA() {
        CostComputer costComputer = new CostComputer();
        Queue<Integer> queue = new LinkedList<>();
        HashMap<Integer, Tuple4<Triple, Double, Double, Double>> processed = new HashMap<>();
        HashMap<Integer, Tuple4<Triple, Double, Double, Double>> remaining = new HashMap<>();
        //List<Tuple4<Triple, Double, Double, Double>> tuple4List = generateRanksMappedTriples(triples, shapeStatsOfTriple);
        HashMap<Triple, Tuple4<Triple, Double, Double, Double>> tempHashMap = firstStar.tripleWithStats.stream().
                collect(Collectors.toMap(tws -> tws.triple, tws -> new Tuple4<>(tws.triple, tws.cardinality, tws.distinctSubjects, tws.distinctObjects), (a, b) -> b, HashMap::new));
        
        //Adjust the remaining and processed lists members according to the triples of the first star
        for (int i = 0; i < tripleWithStats.size(); i++) {
            if (tempHashMap.containsValue(tripleWithStats.get(i))) {
                processed.put(i, tripleWithStats.get(i));
                queue.add(i);
            } else {
                remaining.put(i, tripleWithStats.get(i));
            }
        }
        
        double globalCost = firstStar.starCardinality;
        double sumGlobalCost = firstStar.starCardinality;
        for (int pointer = 0; pointer < tripleWithStats.size() - firstStar.tripleWithStats.size(); pointer++) {
            int i;
            Tuple3<Integer, Tuple4<Triple, Double, Double, Double>, Double> tripleWithCostAndIndex = null;
            Queue<Integer> localQueue = new LinkedList<>(queue);
            
            while (localQueue.size() != 0) {
                //System.out.println(localQueue.size());
                Tuple3<Integer, Tuple4<Triple, Double, Double, Double>, Double> local = costComputer.computeCost(localQueue.poll(), remaining, processed, globalCost);
                if (tripleWithCostAndIndex == null) {
                    tripleWithCostAndIndex = local;
                } else {
                    if (tripleWithCostAndIndex._3 > local._3)
                        tripleWithCostAndIndex = local;
                }
            }
            assert tripleWithCostAndIndex != null;
            globalCost = tripleWithCostAndIndex._3;
            //System.out.println("Global Cost " + globalCost);
            i = tripleWithCostAndIndex._1;
            processed.put(i, tripleWithCostAndIndex._2);
            System.out.println(processed.get(i).toSpecialFormat() + "\t --> " + globalCost);
            //adding processed triple to queue
            //System.out.println("Global Cost for this iteration: " + globalCost);
            queue.add(i);
            remaining.remove(i);
            sumGlobalCost += globalCost;
        }
        System.out.println("Total Cost: joinOrderingViaStarFirstApproach " + sumGlobalCost);
        BasicPattern bp = new BasicPattern();
        for (Integer integer : queue) {
            bp.add(processed.get(integer)._1);
        }
        //this.globalPlanCost = globalCost;
        this.globalPlanCost = sumGlobalCost;
        return queryUtils.makeQueryAlgebra(query, bp, projectedVariables);
    }
    
    public double getGlobalPlanCost() {
        return globalPlanCost;
    }
    
    public List<Star> getStars() {
        return stars;
    }
}

/*
//Calculate the star weight:
// 1. Iterate over the triples of the star,
// 2. Use the triplesWithStats HashMap to get the individual weight of the triple i.e., size of ArrayList for each key of the HashMap
// 3. Take an average and assign the weight to this star
//long triplesTotalWeight = star.triples.stream().filter(t -> triplesWithStats.get(t.triple) != null).mapToLong(t -> triplesWithStats.get(t.triple).size()).sum();
//star.starWeight = triplesTotalWeight / star.triples.size();*/