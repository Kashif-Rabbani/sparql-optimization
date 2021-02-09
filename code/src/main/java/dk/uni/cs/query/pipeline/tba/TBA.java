package dk.uni.cs.query.pipeline.tba;

import dk.uni.cs.extras.TrueCardinalityComputer;
import dk.uni.cs.query.pipeline.CostComputer;
import dk.uni.cs.query.pipeline.QueryUtils;
import dk.uni.cs.query.pipeline.TriplesEvaluatorWithPlan;
import dk.uni.cs.query.pipeline.sba.SBA;
import dk.uni.cs.query.pipeline.sba.Star;
import dk.uni.cs.shapes.Statistics;
import dk.uni.cs.utils.ConfigManager;
import dk.uni.cs.utils.Tuple3;
import dk.uni.cs.utils.Tuple4;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.vocabulary.RDF;
import dk.uni.cs.utils.Log;

import java.text.DecimalFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TBA {
    private final boolean LOG_TRUE_CARDINALITIES = Boolean.parseBoolean(ConfigManager.getProperty("logTrueCardinalities"));
    QueryUtils queryUtils = new QueryUtils();
    CostComputer costComputer = new CostComputer();
    List<Tuple4<Triple, Double, Double, Double>> orderedPlanTriples = new ArrayList<>();
    HashMap<Triple, Boolean> isTriplePatternTypeDefined = new HashMap<>();
    HashMap<Triple, Tuple4<Triple, Double, Double, Double>> tpCardDscDocHashMap = new HashMap<>();
    HashMap<Triple, Tuple4<Triple, Double, Double, Double>> tpGlobalCardDscDocHashMap = new HashMap<>();
    Query query;
    String queryId;
    List<Star> stars = null;
    HashSet<Triple> triples;
    HashMap<Triple, List<Statistics>> triplesWithStats;
    List<Var> projectedVariables;
    double globalPlanCost;
    Triple recommendedFirstTriple = null;
    DecimalFormat formatter = new DecimalFormat("#,###.00");
    HashMap<Triple, Double> tripleEstimatedCard;
    
    public TBA(Query query, HashSet<Triple> triples, HashMap<Triple, List<Statistics>> triplesWithStats, HashMap<Triple, Boolean> isTriplePatternTypeDefined) {
        this.query = query;
        this.triples = triples;
        this.triplesWithStats = triplesWithStats;
        this.projectedVariables = query.getProjectVars();
        this.isTriplePatternTypeDefined = isTriplePatternTypeDefined;
    }
    
    public void costComputationWithFixedOrdering(List<Triple> triplePatterns) {
        
        CostComputer costComputer = new CostComputer();
        
        List<TheTriple> enrichedTriples = new ArrayList<>();
        
        double costGlobalPlan = 0;
        double cardGlobalPlan = 0;
        double costShapeSpecificPlan = 0;
        double cardShapeSpecificPlan = 0;
        
        //Shape Specific Card, DSC, DOC of triple fetching from SHACL Shapes
        estimateTripleCardDscDoc(triples, triplesWithStats, true);
        
        //Global Specific Card, DSC, DOC of triple
        estimateTripleCardDscDoc(triples, triplesWithStats, false);
        
        //given the current order of triple patterns, compute the cost and final estimated cardinality for execution in this order
        // let's try with global stats first
        double intermediateCard = 0;
        double cost = 0;
        for (int i = 0, triplePatternsSize = triplePatterns.size(); i < triplePatternsSize - 1; i++) {
            //Triple triple = triplePatterns.get(i);
            Tuple4<Triple, Double, Double, Double> tripleA = tpGlobalCardDscDocHashMap.get(triplePatterns.get(i));
            Tuple4<Triple, Double, Double, Double> tripleB = tpGlobalCardDscDocHashMap.get(triplePatterns.get(i + 1));
            
            if (i == 0)
                intermediateCard = tripleA._2;
            
            //compute the join cost between A & B
            intermediateCard = costComputer.estimateJoinCostForTwoTriplePatterns(tripleA, tripleB, intermediateCard);
            cost += intermediateCard;
        }
        System.out.println("GS: Final Estimated Cardinality: " + intermediateCard + ", Cost: " + cost);
        costGlobalPlan = cost;
        cardGlobalPlan = intermediateCard;
        
        
        for (int i = 0, triplePatternsSize = triplePatterns.size(); i < triplePatternsSize; i++) {
            boolean rdfTypeFlag = false;
            if ((triplePatterns.get(i)).getPredicate().toString().equals(RDF.type.toString())) {
                rdfTypeFlag = true;
            }
            Star theTriplesStar = null;
            for (Star star : stars) {
                if (star.triples.contains(triplePatterns.get(i))) {
                    theTriplesStar = star;
                }
            }
            enrichedTriples.add(new TheTriple(i, triplePatterns.get(i), rdfTypeFlag, theTriplesStar, tpGlobalCardDscDocHashMap.get(triplePatterns.get(i))));
        }
        
        updateTriplesWithShapesStats(enrichedTriples);
        
        boolean reorderUsingShapeStats = false;
        boolean reorderUsingShapeStatsSpecial = false;
        
        List<TheTriple> rdfTypeTriples = enrichedTriples.stream().filter(theTriple -> theTriple.rdfType).collect(Collectors.toList());
        if (rdfTypeTriples.size() >= 1) {
            for (TheTriple rdfTypeTriple : rdfTypeTriples) {
                if (rdfTypeTriples.size() == 1 && rdfTypeTriple.index == enrichedTriples.size()) {
                    //No need to do anything as the RDF.type triple is already at the end and it can not be modified and there is only one RDf.type triple
                    System.out.println("No need for further shape specific optimization!");
                    break;
                }
                if (rdfTypeTriple.index == 0) {
                    //First Triple Pattern is of RDF.type: Reorder the plan using shape specific statistics
                    reorderUsingShapeStats = true;
                    break;
                } else {
                    reorderUsingShapeStatsSpecial = true;
                    break;
                }
            }
        }
        
        if (reorderUsingShapeStats || reorderUsingShapeStatsSpecial) {
            
            HashMap<Triple, Tuple4<Triple, Double, Double, Double>> hybridCardMap = new HashMap<>();
            enrichedTriples.forEach(theTriple -> {
                hybridCardMap.put(theTriple.triple, theTriple.getStats());
            });
            cost = 0;
            intermediateCard = 0;
            
            for (int i = 0, triplePatternsSize = triplePatterns.size(); i < triplePatternsSize - 1; i++) {
                //Triple triple = triplePatterns.get(i);
                Tuple4<Triple, Double, Double, Double> tripleA = hybridCardMap.get(triplePatterns.get(i));
                Tuple4<Triple, Double, Double, Double> tripleB = hybridCardMap.get(triplePatterns.get(i + 1));
                
                if (i == 0)
                    intermediateCard = tripleA._2;
                
                //compute the join cost between A & B
                intermediateCard = costComputer.estimateJoinCostForTwoTriplePatterns(tripleA, tripleB, intermediateCard);
                cost += intermediateCard;
            }
            System.out.println("SS, Final Estimated Cardinality: " + intermediateCard + ", Cost: " + cost);
            costShapeSpecificPlan = cost;
            cardShapeSpecificPlan = intermediateCard; // this is the final estimated card
        }
        TrueCardinalityComputer tc = new TrueCardinalityComputer(triplePatterns);
        System.out.println("STATS_OUTPUT,query,trueFinalCard,trueCost,gsEstimatedCard,gsEstimatedCost,ssEstimatedCard,ssEstimatedCost");
        System.out.println("STATS_OUTPUT," + queryId + "," + tc.finalCard + "," + tc.sumCard + "," + cardGlobalPlan + "," + costGlobalPlan + "," + cardShapeSpecificPlan + "," + costShapeSpecificPlan);
        tc.triplesWithCard.forEach(System.out::println);
    }
    
    public Op opTBA_V1() {
        HashMap<Integer, Tuple4<Triple, Double, Double, Double>> processed = new HashMap<>();
        Queue<Integer> queue = new LinkedList<>();
        
        //Shape Specific Card, DSC, DOC of triple
        List<Tuple4<Triple, Double, Double, Double>> tripleCardDscDoc = estimateTripleCardDscDoc(triples, triplesWithStats, false);
        
        double sumGlobalCost = ordering(costComputer, processed, queue, tripleCardDscDoc);
        
        BasicPattern bp = new BasicPattern();
        
        for (Integer val : queue) {
            bp.add(processed.get(val)._1);
            orderedPlanTriples.add(processed.get(val));
        }
        
        System.out.println("Total Cost: " + sumGlobalCost);
        this.globalPlanCost = sumGlobalCost;
        return queryUtils.makeQueryAlgebra(query, bp, projectedVariables);
    }
    
    public Op opTBA_V2() {
        BasicPattern bp = new BasicPattern();
        CostComputer costComputer = new CostComputer();
        HashMap<Integer, Tuple4<Triple, Double, Double, Double>> processed = new HashMap<>();
        List<Tuple4<Triple, Double, Double, Double>> hybridTripleCardList;
        Queue<Integer> queue = new LinkedList<>();
        
        List<TheTriple> enrichedTriples = new ArrayList<>();
        List<String> globalPlanEstimation = new ArrayList<>();
        List<String> shapesPlanEstimation = new ArrayList<>();
        
        List<Triple> globalPlanTriples = new ArrayList<>();
        List<Triple> shapesPlanTriples = new ArrayList<>();
        
        
        double costGlobalPlan = 0;
        double costShapeSpecificPlan = 0;
        
        //Shape Specific Card, DSC, DOC of triple fetching from SHACL Shapes
        List<Tuple4<Triple, Double, Double, Double>> tripleGlobalCardDscDoc = estimateTripleCardDscDoc(triples, triplesWithStats, true);
        
        //Global Specific Card, DSC, DOC of triple
        estimateTripleCardDscDoc(triples, triplesWithStats, false);
        
        ordering(costComputer, processed, queue, tripleGlobalCardDscDoc);
        
        //iterate over the current join ordering proposed by Baseline approach and prepare the data structure
        
        int tracker = 0;
        for (Integer val : queue) {
            bp.add(processed.get(val)._1);
            globalPlanEstimation.add(processed.get(val) + "," + tripleEstimatedCard.get(processed.get(val)._1).toString());
            globalPlanTriples.add(processed.get(val)._1);
            if (val != 0) {
                costGlobalPlan += tripleEstimatedCard.get(processed.get(val)._1);
            }
            orderedPlanTriples.add(processed.get(val));
            boolean rdfTypeFlag = false;
            if (processed.get(val)._1.getPredicate().toString().equals(RDF.type.toString())) {
                rdfTypeFlag = true;
            }
            Star theTriplesStar = null;
            for (Star star : stars) {
                if (star.triples.contains(processed.get(val)._1)) {
                    theTriplesStar = star;
                }
            }
            enrichedTriples.add(new TheTriple(tracker, processed.get(val)._1, rdfTypeFlag, theTriplesStar, tpGlobalCardDscDocHashMap.get(processed.get(val)._1())));
            tracker++;
        }
        
        
        updateTriplesWithShapesStats(enrichedTriples);
        
        hybridTripleCardList = enrichedTriples.stream().map(TheTriple::getStats).collect(Collectors.toList());
        
        boolean reorderUsingShapeStats = false;
        boolean reorderUsingShapeStatsSpecial = false;
        
        List<TheTriple> rdfTypeTriples = enrichedTriples.stream().filter(theTriple -> theTriple.rdfType).collect(Collectors.toList());
        if (rdfTypeTriples.size() >= 1) {
            for (TheTriple rdfTypeTriple : rdfTypeTriples) {
                if (rdfTypeTriples.size() == 1 && rdfTypeTriple.index == enrichedTriples.size()) {
                    //No need to do anything as the RDF.type triple is already at the end and it can not be modified and there is only one RDf.type triple
                    System.out.println("No need for further shape specific optimization!");
                    break;
                }
                if (rdfTypeTriple.index == 0) {
                    //First Triple Pattern is of RDF.type: Reorder the plan using shape specific statistics
                    reorderUsingShapeStats = true;
                    break;
                } else {
                    reorderUsingShapeStatsSpecial = true;
                    break;
                }
            }
        }
        
        
        if (reorderUsingShapeStats) {
            System.out.println("OPTIMIZATION AT SHAPE LEVEL - 1 : OPTIMIZING USING SHAPES STATISTICS");
            processed = new HashMap<>();
            queue = new LinkedList<>();
            costComputer = new CostComputer();
            bp = new BasicPattern();
            costShapeSpecificPlan = 0;
            double sumOfGlobalCostNewMethod = ordering(costComputer, processed, queue, hybridTripleCardList);
            for (Integer val : queue) {
                bp.add(processed.get(val)._1);
                shapesPlanEstimation.add(processed.get(val) + "," + tripleEstimatedCard.get(processed.get(val)._1).toString());
                shapesPlanTriples.add(processed.get(val)._1);
                costShapeSpecificPlan += tripleEstimatedCard.get(processed.get(val)._1);
                orderedPlanTriples.add(processed.get(val));
            }
            this.globalPlanCost = sumOfGlobalCostNewMethod;
        }
        
        if (reorderUsingShapeStatsSpecial) {
            System.out.println("OPTIMIZATION AT SHAPE LEVEL - 2 : OPTIMIZING USING SHAPES STATISTICS");
            processed = new HashMap<>();
            queue = new LinkedList<>();
            costComputer = new CostComputer();
            bp = new BasicPattern();
            
            TheTriple firstRdfTypeTriple = rdfTypeTriples.stream().min(Comparator.comparing(TheTriple::getIndex)).get();
            for (int i = 0; i <= firstRdfTypeTriple.index; i++) {
                System.out.println(enrichedTriples.get(i).triple);
                processed.put(i, enrichedTriples.get(i).getStats());
            }
            orderingSpecial(costComputer, processed, queue, hybridTripleCardList);
            
            costShapeSpecificPlan = 0;
            for (Integer val : queue) {
                bp.add(processed.get(val)._1);
                shapesPlanEstimation.add(processed.get(val) + "," + tripleEstimatedCard.get(processed.get(val)._1).toString());
                shapesPlanTriples.add(processed.get(val)._1);
                if (val != 0) {
                    costShapeSpecificPlan += tripleEstimatedCard.get(processed.get(val)._1);
                }
                orderedPlanTriples.add(processed.get(val));
            }
        }
        
        
        System.out.println("Join Ordering with cardinality estimates for GS:");
        globalPlanEstimation.forEach(System.out::println);
        System.out.println("COST - Sum of cardinality estimates: " + costGlobalPlan);
        
        System.out.println();
        System.out.println();
        
        System.out.println("Join Ordering with cardinality estimates for SS:");
        shapesPlanEstimation.forEach(System.out::println);
        System.out.println("COST - Sum of cardinality estimates: " + costShapeSpecificPlan);
        System.out.println();
        
        String cardRow = "";
        String costRow = "";
        
        if (reorderUsingShapeStats || reorderUsingShapeStatsSpecial) {
            System.out.println("CardinalityEstimation," + queryId + "," + (globalPlanEstimation.get(globalPlanEstimation.size() - 1)).split(",")[4] + "," + (shapesPlanEstimation.get(shapesPlanEstimation.size() - 1)).split(",")[4]);
            cardRow += "," + (globalPlanEstimation.get(globalPlanEstimation.size() - 1)).split(",")[4] + "," + (shapesPlanEstimation.get(shapesPlanEstimation.size() - 1)).split(",")[4] + ",";
        } else {
            System.out.println("CardinalityEstimation," + queryId + "," + (globalPlanEstimation.get(globalPlanEstimation.size() - 1)).split(",")[4] + "," + "NULL");
            cardRow += "," + (globalPlanEstimation.get(globalPlanEstimation.size() - 1)).split(",")[4] + "," + "NULL";
        }
        System.out.println("QueryCost," + queryId + "," + costGlobalPlan + "," + costShapeSpecificPlan);
        
        if (LOG_TRUE_CARDINALITIES) {
            costRow += costGlobalPlan + "," + costShapeSpecificPlan + ",";
            System.out.println("GS_PLAN");
            TrueCardinalityComputer gTCC = new TrueCardinalityComputer(globalPlanTriples);
            cardRow = "STATS_OUTPUT," + queryId + "," + gTCC.finalCard + cardRow;
            costRow += gTCC.sumCard + ",";
            
            TrueCardinalityComputer sTCC = null;
            if (!shapesPlanTriples.isEmpty()) {
                sTCC = new TrueCardinalityComputer(shapesPlanTriples);
                cardRow += sTCC.sumCard;
            }
            gTCC.triplesWithCard.forEach(System.out::println);
            if (sTCC != null) {
                System.out.println("SS_PLAN");
                sTCC.triplesWithCard.forEach(System.out::println);
            }
            
            System.out.println("STATS_OUTPUT,query,trueFinalCard,gsEstimatedCard,ssEstimatedCard,gsEstimatedCost,ssEstimatedCost,gsTrueCost,ssTrueCost");
            System.out.println(cardRow + "," + costRow);
        }
        
        return queryUtils.makeQueryAlgebra(query, bp, projectedVariables);
    }
    
    private void updateTriplesWithShapesStats(List<TheTriple> enrichedTriples) {
        //Goal is to change the stats of a triple pattern to its shape specific stats if it has occurred after the
        for (int i = 0; i < enrichedTriples.size(); i++) {
            TheTriple t = enrichedTriples.get(i);//for each triple you should check if its type is defined
            //if yes, then iterate over the remaining triples and get the triples that belong to the same star and change their stats
            if (t.rdfType) {
                for (int j = i + 1; j < enrichedTriples.size(); j++) {
                    if (enrichedTriples.get(j).star.equals(t.star)) {
                        //System.out.println("Change Stats of " + enrichedTriples.get(j).triple);
                        enrichedTriples.get(j).setStats(tpCardDscDocHashMap.get(enrichedTriples.get(j).triple));
                    }
                }
            }
        }
    }
    
    public Op opTBA_GlobalStats() {
        HashMap<Integer, Tuple4<Triple, Double, Double, Double>> processed = new HashMap<>();
        Queue<Integer> queue = new LinkedList<>();
        
        //Global Statistics: Card, DSC, DOC of triple
        List<Tuple4<Triple, Double, Double, Double>> tripleCardDscDoc = estimateTripleCardDscDoc(triples, triplesWithStats, true);
        
        double sumGlobalCost = ordering(costComputer, processed, queue, tripleCardDscDoc);
        
        BasicPattern bp = new BasicPattern();
        
        for (Integer val : queue) {
            bp.add(processed.get(val)._1);
            orderedPlanTriples.add(processed.get(val));
        }
        
        System.out.println("Total Cost: " + sumGlobalCost);
        this.globalPlanCost = sumGlobalCost;
        return queryUtils.makeQueryAlgebra(query, bp, projectedVariables);
    }
    
    private double ordering(CostComputer costComputer, HashMap<Integer, Tuple4<Triple, Double, Double, Double>> processed, Queue<Integer> queue, List<Tuple4<Triple, Double, Double, Double>> tripleCardDscDoc) {
        HashMap<Integer, Tuple4<Triple, Double, Double, Double>> remaining = IntStream.range(0, tripleCardDscDoc.size()).boxed().collect(Collectors.toMap(i -> i, tripleCardDscDoc::get, (a, b) -> b, HashMap::new));
        HashMap<Triple, Integer> remTriplePosition = new HashMap<>();
        tripleEstimatedCard = new HashMap<>();
        remaining.forEach((key, tuple) -> {
            remTriplePosition.put(tuple._1, key);
        });
        
        int i = 0;
        double globalCost = 0;
        double sumGlobalCost = 0;
        for (int pointer = 0; pointer < tripleCardDscDoc.size(); pointer++) {
            if (pointer == 0) {
                processed.put(0, tripleCardDscDoc.get(0));
                Log.consolePrint(tripleCardDscDoc.get(0).toSpecialFormat() + "\t -- EC: " + formatter.format(tripleCardDscDoc.get(0)._2));
                globalCost = tripleCardDscDoc.get(0)._2;
                tripleEstimatedCard.put(tripleCardDscDoc.get(0)._1, tripleCardDscDoc.get(0)._2);
            }
            queue.add(i);
            remaining.remove(i);
            
            Tuple3<Integer, Tuple4<Triple, Double, Double, Double>, Double> tripleWithCostAndIndex = null;
            // 1st pair of join-ordering is decided here
            if (pointer == 0) {
                tripleWithCostAndIndex = costComputer.computeCost(i, remaining, processed, globalCost);
                /*HashMap<Join, Double> joinCostForFirstPair = new HashMap<>(costComputer.getJoinCost());
                (joinCostForFirstPair.entrySet().stream().sorted(Map.Entry.comparingByValue()).collect(Collectors.toList())).forEach(joinDoubleEntry -> {
                    System.out.print(joinDoubleEntry.getKey() + " -> " + formatter.format(joinDoubleEntry.getValue()) + "\n");
                });*/
                globalCost = tripleWithCostAndIndex._3;
                Log.logLine("Global Cost: " + globalCost);
            } else {
                Queue<Integer> localQueue = new LinkedList<>(queue);
                HashMap<Join, Double> joinCostForCurrentQueue = new HashMap<>();
                while (localQueue.size() != 0) {
                    Log.logLine(" ---> " + localQueue.size());
                    int pointerToPass = localQueue.poll();
                    Tuple3<Integer, Tuple4<Triple, Double, Double, Double>, Double> local = costComputer.computeCost(pointerToPass, remaining, processed, globalCost);
                    //Log.logLine("Local Triple returned: " + local);
                    if (tripleWithCostAndIndex == null) {
                        tripleWithCostAndIndex = local;
                    } else {
                        if (tripleWithCostAndIndex._3 > local._3)
                            tripleWithCostAndIndex = local;
                    }
                    /*costComputer.getJoinCost().forEach((join, cost) -> {Log.logLine(join + " --> " + cost);});*/
                    joinCostForCurrentQueue.putAll(costComputer.getJoinCost());
                }
                
                //Log.logLine("Join HashMap of current Queue: ");
                //joinCostForCurrentQueue.forEach((join, cost) -> { Log.logLine(join + " -> " + cost); });
                
                Log.logLine("Grouping....");
                // group by the result in the order of remaining triples
                Set<Pair<Pair<Triple, Triple>, Double>> pairSet = new HashSet<>();
                joinCostForCurrentQueue.forEach((joinOuter, costOuter) -> {
                    joinCostForCurrentQueue.forEach((joinInner, costInner) -> {
                        if (joinOuter.tripleB.toString().equals(joinInner.tripleB.toString()) && !pairSet.contains(Pair.create(Pair.create(joinInner.tripleA, joinInner.tripleB), costInner))) {
                            //Log.logLine(Pair.create(Pair.create(joinInner.tripleA, joinInner.tripleB), costInner));
                            pairSet.add(Pair.create(Pair.create(joinInner.tripleA, joinInner.tripleB), costInner));
                        }
                    });
                });
                Map<Triple, List<Pair<Pair<Triple, Triple>, Double>>> groups = pairSet.stream().collect(Collectors.groupingBy(pairDoublePair -> pairDoublePair.getLeft().getRight()));
                Set<Pair<Pair<Triple, Triple>, Double>> worstCaseJoins = new HashSet<>();
                groups.forEach((triple, pairs) -> {
                    worstCaseJoins.add(pairs.stream().min(Comparator.comparing(Pair::getRight)).get());
                });
                Log.logLine("Worst Case Join Pairs:");
                worstCaseJoins.forEach(pairDoublePair -> {
                    Log.logLine(pairDoublePair.getLeft().toString() + " --> " + pairDoublePair.getRight());
                });
                if (remaining.size() > 0) {
                    Pair<Pair<Triple, Triple>, Double> chosenJoinPair = worstCaseJoins.parallelStream().min(Comparator.comparing(Pair::getRight)).get();
                    tripleWithCostAndIndex = new Tuple3<>(remTriplePosition.get(chosenJoinPair.getLeft().getRight()), remaining.get(remTriplePosition.get(chosenJoinPair.getLeft().getRight())), chosenJoinPair.getRight());
                    Log.logLine("The chosen Join Pair: " + chosenJoinPair);
                }
                globalCost = tripleWithCostAndIndex._3;
                Log.logLine("Global Cost " + globalCost);
            }
            assert tripleWithCostAndIndex != null;
            i = tripleWithCostAndIndex._1;
            
            if (i != 0) {
                processed.put(i, tripleWithCostAndIndex._2);
                Log.consolePrint(processed.get(i).toSpecialFormat() + "\t -- EC: " + formatter.format(globalCost));
                tripleEstimatedCard.put(processed.get(i)._1, globalCost);
                sumGlobalCost += globalCost;
            }
        }
        return sumGlobalCost;
    }
    
    private void orderingSpecial(CostComputer costComputer, HashMap<Integer, Tuple4<Triple, Double, Double, Double>> processed, Queue<Integer> queue, List<Tuple4<Triple, Double, Double, Double>> tripleCardDscDoc) {
        
        HashMap<Integer, Tuple4<Triple, Double, Double, Double>> remaining = IntStream.range(0, tripleCardDscDoc.size()).boxed().collect(Collectors.toMap(i -> i, tripleCardDscDoc::get, (a, b) -> b, HashMap::new));
        for (int i = 0; i < processed.size() - 1; i++) {
            remaining.remove(i);
            queue.add(i);
        }
        HashMap<Triple, Integer> remTriplePosition = new HashMap<>();
        remaining.forEach((key, tuple) -> { remTriplePosition.put(tuple._1, key); });
        
        int i = processed.size();
        double globalCost = this.tripleEstimatedCard.get(processed.get(processed.size() - 1)._1);
        double sumGlobalCost = globalCost;
        boolean initStep = true;
        for (int pointer = processed.size(); pointer <= tripleCardDscDoc.size(); pointer++) {
            if (initStep) {
                queue.add(i - 1);
                remaining.remove(i - 1);
                initStep = false;
            } else {
                queue.add(i);
                remaining.remove(i);
            }
            Tuple3<Integer, Tuple4<Triple, Double, Double, Double>, Double> tripleWithCostAndIndex = null;
            
            Queue<Integer> localQueue = new LinkedList<>(queue);
            HashMap<Join, Double> joinCostForCurrentQueue = new HashMap<>();
            while (localQueue.size() != 0) {
                Log.logLine(" ---> " + localQueue.size());
                int pointerToPass = localQueue.poll();
                Tuple3<Integer, Tuple4<Triple, Double, Double, Double>, Double> local = costComputer.computeCost(pointerToPass, remaining, processed, globalCost);
                //Log.logLine("Local Triple returned: " + local);
                if (tripleWithCostAndIndex == null) {
                    tripleWithCostAndIndex = local;
                } else {
                    if (tripleWithCostAndIndex._3 > local._3)
                        tripleWithCostAndIndex = local;
                }
                /*costComputer.getJoinCost().forEach((join, cost) -> {Log.logLine(join + " --> " + cost);});*/
                joinCostForCurrentQueue.putAll(costComputer.getJoinCost());
            }
            
            //Log.logLine("Join HashMap of current Queue: ");
            //joinCostForCurrentQueue.forEach((join, cost) -> { Log.logLine(join + " -> " + cost); });
            
            Log.logLine("Grouping....");
            // group by the result in the order of remaining triples
            Set<Pair<Pair<Triple, Triple>, Double>> pairSet = new HashSet<>();
            joinCostForCurrentQueue.forEach((joinOuter, costOuter) -> {
                joinCostForCurrentQueue.forEach((joinInner, costInner) -> {
                    if (joinOuter.tripleB.toString().equals(joinInner.tripleB.toString()) && !pairSet.contains(Pair.create(Pair.create(joinInner.tripleA, joinInner.tripleB), costInner))) {
                        //Log.logLine(Pair.create(Pair.create(joinInner.tripleA, joinInner.tripleB), costInner));
                        pairSet.add(Pair.create(Pair.create(joinInner.tripleA, joinInner.tripleB), costInner));
                    }
                });
            });
            Map<Triple, List<Pair<Pair<Triple, Triple>, Double>>> groups = pairSet.stream().collect(Collectors.groupingBy(pairDoublePair -> pairDoublePair.getLeft().getRight()));
            Set<Pair<Pair<Triple, Triple>, Double>> worstCaseJoins = new HashSet<>();
            groups.forEach((triple, pairs) -> {
                worstCaseJoins.add(pairs.stream().min(Comparator.comparing(Pair::getRight)).get());
            });
            Log.logLine("Worst Case Join Pairs:");
            worstCaseJoins.forEach(pairDoublePair -> {
                Log.logLine(pairDoublePair.getLeft().toString() + " --> " + pairDoublePair.getRight());
            });
            if (remaining.size() > 0) {
                Pair<Pair<Triple, Triple>, Double> chosenJoinPair = worstCaseJoins.parallelStream().min(Comparator.comparing(Pair::getRight)).get();
                tripleWithCostAndIndex = new Tuple3<>(remTriplePosition.get(chosenJoinPair.getLeft().getRight()), remaining.get(remTriplePosition.get(chosenJoinPair.getLeft().getRight())), chosenJoinPair.getRight());
                Log.logLine("The chosen Join Pair: " + chosenJoinPair);
            }
            globalCost = tripleWithCostAndIndex._3;
            Log.logLine("Global Cost " + globalCost);
            
            assert tripleWithCostAndIndex != null;
            i = tripleWithCostAndIndex._1;
            
            if (i != 0) {
                processed.put(i, tripleWithCostAndIndex._2);
                Log.consolePrint(processed.get(i).toSpecialFormat() + "\t -- EC: " + formatter.format(globalCost));
                tripleEstimatedCard.put(processed.get(i)._1, globalCost);
                sumGlobalCost += globalCost;
            }
        }
    }
    
    private List<Tuple4<Triple, Double, Double, Double>> estimateTripleCardDscDoc(HashSet<Triple> triples, HashMap<Triple, List<Statistics>> shapeStatsOfTriple, Boolean globalFlag) {
        TripleRankComputer tripleRankComputer = new TripleRankComputer(this);
        List<Tuple4<Triple, Double, Double, Double>> tuple4List = new ArrayList<Tuple4<Triple, Double, Double, Double>>();
        // go through each triple one by one
        for (Triple triple : triples) {
            List<Statistics> shapes = shapeStatsOfTriple.get(triple);
            if (shapes.size() == 0) {
                System.out.println("Statistics from the shape are zero for this " + triple);
            }
            double rank = 0;
            if (globalFlag)
                rank = getGlobalRank(tripleRankComputer, triple, shapes, rank);
            else
                rank = getShapeSpecificRank(tripleRankComputer, triple, shapes, rank);
            
            double dsc = tripleRankComputer.getCountDistinctSubjects();
            double doc = tripleRankComputer.getCountDistinctObjects();
            
            if (globalFlag) {
                tpGlobalCardDscDocHashMap.put(triple, new Tuple4<>(triple, rank, dsc, doc));
            } else {
                tpCardDscDocHashMap.put(triple, new Tuple4<>(triple, rank, dsc, doc));
            }
            tuple4List.add(new Tuple4<>(triple, rank, dsc, doc));
        }
        
        tuple4List = tuple4List.stream().sorted(Comparator.comparing(Tuple4::_2)).collect(Collectors.toList());
        
        if (!globalFlag && recommendedFirstTriple != null) {
            int thePosition = 0;
            for (int i = 0, tuple4ListSize = tuple4List.size(); i < tuple4ListSize; i++) {
                if (tuple4List.get(i)._1().equals(recommendedFirstTriple)) {
                    thePosition = i;
                }
            }
            Collections.swap(tuple4List, 0, thePosition);
        }
        return tuple4List;
    }
    
    private double getShapeSpecificRank(TripleRankComputer tripleRankComputer, Triple triple, List<Statistics> shapes, double rank) {
        // ************************ CASE A *******************************
        if (!triple.getObject().isVariable()) {
            rank = tripleRankComputer.getRankWhenObjIsNotVar(shapes, triple, rank);
        }
        // ************************  CASE B *******************************
        if (triple.getObject().isVariable()) {
            rank = tripleRankComputer.getRankWhenObjIsVar(shapes, triple, rank);
        }
        return rank;
    }
    
    private double getGlobalRank(TripleRankComputer tripleRankComputer, Triple triple, List<Statistics> shapes, double rank) {
        // ************************ CASE A *******************************
        if (!triple.getObject().isVariable()) {
            rank = tripleRankComputer.getGlobalRankWhenObjIsNotVar(shapes, triple, rank);
        }
        // ************************  CASE B *******************************
        if (triple.getObject().isVariable()) {
            rank = tripleRankComputer.getGlobalRankWhenObjIsVar(shapes, triple, rank);
        }
        return rank;
    }
    
    public double getGlobalPlanCost() {
        return globalPlanCost;
    }
    
    //setter method
    
    public void setStarsInfo(SBA sba) {
        this.stars = sba.getStars().stream().sorted(Comparator.comparing(Star::getValueOfLeastCard)).collect(Collectors.toList());
        //System.out.println("Printing RDF type triple of the star having a triple with a least cardinality");
        //System.out.println(star.getRdfTypeTriple());
        for (Star star : stars) {
            if (star.isTypeDefined) {
                //System.out.println("Set recommendedFirstTriple as " + star.getRdfTypeTriple());
                this.recommendedFirstTriple = star.getRdfTypeTriple();
                break;
            }
        }
    }
    
    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }
    
    
    //Getter Methods
    public HashMap<Triple, List<Statistics>> getTriplesWithStats() {
        return triplesWithStats;
    }
    
    public HashMap<Triple, Boolean> getIsTriplePatternTypeDefined() { return isTriplePatternTypeDefined; }
    
    public List<Tuple4<Triple, Double, Double, Double>> getOrderedPlanTriples() { return orderedPlanTriples; }
}


/*
List<Triple> list = bp.getList();
System.out.println("THE LIST");
System.out.println(list);
Collections.swap(list, list.size()-2, 4);
System.out.println(list);
*/