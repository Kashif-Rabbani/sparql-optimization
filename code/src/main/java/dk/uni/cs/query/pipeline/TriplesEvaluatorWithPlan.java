package dk.uni.cs.query.pipeline;

import dk.uni.cs.query.pipeline.sba.Star;
import dk.uni.cs.query.pipeline.sba.StarsEvaluator;
import dk.uni.cs.query.pipeline.sba.TripleWithStats;
import dk.uni.cs.shapes.ShapesDetector;
import dk.uni.cs.shapes.Statistics;
import dk.uni.cs.utils.GraphDBUtils;
import dk.uni.cs.utils.Tuple3;
import dk.uni.cs.utils.Tuple4;
import org.apache.jena.arq.querybuilder.SelectBuilder;
import org.apache.jena.ext.com.google.common.base.Functions;
import org.apache.jena.ext.com.google.common.collect.Lists;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.*;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.lang.sparql_11.ParseException;
import org.apache.jena.vocabulary.RDF;
import org.eclipse.rdf4j.query.BindingSet;

import java.text.NumberFormat;
import java.util.*;
import java.util.stream.Collectors;

/*
This approach is called Triples Based Approach in which query is evaluated triple by triple using defined formulas
This class specifically modified to have extended logs of query plan with estimated and real join cardinalities
 */
public class TriplesEvaluatorWithPlan {
    private final boolean extendedLogs = true;
    private final QueryUtils queryUtils = new QueryUtils();
    private String posedQuery = "";
    private List<Var> projectedVariables = null;
    private LinkedHashMap<Triple, String> qpt = new LinkedHashMap<Triple, String>(); // query plan triples (qpt)
    private final GraphDBUtils graphDBUtils = new GraphDBUtils();
    
    public TriplesEvaluatorWithPlan() {
    }
    
    public TriplesEvaluatorWithPlan(String query) {
        this.posedQuery = query;
    }

    public Op designQueryPlan(String queryId) {
        Op algebraOp = null;
        Query query = QueryFactory.create(posedQuery);
        projectedVariables = query.getProjectVars();
        ArrayList<HashSet<Triple>> bgps = QueryUtils.getBGPs(query);
        //for every Basic Graph Pattern
        for (HashSet<Triple> triples : bgps) {
            HashMap<Triple, List<Statistics>> triplesWithStats = new HashMap<>();
            HashMap<Triple, List<Tuple3<String, String, String>>> triplesToShapesMapping = new ShapesDetector(triples, true).getCandidateShapesOfTriples();

            // Get statistics
            queryUtils.getStatisticsFromShapes(triplesToShapesMapping, triplesWithStats);

            // extract stars and get the star with the least cardinality, we will use the stars extractor function from Triples Based Approach - class StarsEvaluator
            StarsEvaluator starsEvaluator = new StarsEvaluator();
            Vector<SubQuery> stars = starsEvaluator.getStars(triples, triplesWithStats);
            List<Star> allStarsWithStats = starsEvaluator.iterateSubQueriesToExtractStarsWithStats(stars, query);
            System.out.println("  >> Query BGP Stars: \n");
            System.out.println(allStarsWithStats);

            List<TripleWithStats> tws = new ArrayList<>();
            for (Star star : allStarsWithStats) {
                tws.addAll(star.tripleWithStats);
            }
            List<Tuple4<Triple, Double, Double, Double>> t4list = new ArrayList<>();
            for (TripleWithStats t : tws) {
                //new Tuple4<>(triple, rank, dsc, doc);
                t4list.add(new Tuple4<>(t.triple, t.cardinality, t.distinctSubjects, t.distinctObjects));
            }
            //System.out.println(t4list);
            algebraOp = calculateTriplesOrderWithFirstStar(query, t4list, triplesWithStats, allStarsWithStats.stream().sorted(Comparator.comparing(Star::getStarCardinality)).collect(Collectors.toList()).get(0));

            //algebraOp = calculateTriplesOrder(query, triples, triplesWithStats);
            System.out.println(algebraOp);
        }

        logEstimatedAndActualJoinCardinalityOfQueryPlan(queryId);
        return algebraOp;
    }

    private Op calculateTriplesOrderWithFirstStar(Query query, List<Tuple4<Triple, Double, Double, Double>> tuple4List, HashMap<Triple, List<Statistics>> shapeStatsOfTriple, Star firstStar) {

        System.out.println("\n >> Selected First Star to start query plan with...  " + firstStar);

        System.out.println("\n\n >> Started computing estimates to find a query plan: \n");
        manageEstimatesForStarTPsForLogging(firstStar);

        Queue<Integer> queue = new LinkedList<>();
        HashMap<Integer, Tuple4<Triple, Double, Double, Double>> processed = new HashMap<>();
        HashMap<Integer, Tuple4<Triple, Double, Double, Double>> remaining = new HashMap<>();
        //List<Tuple4<Triple, Double, Double, Double>> tuple4List = generateRanksMappedTriples(triples, shapeStatsOfTriple);
        HashMap<Triple, Tuple4<Triple, Double, Double, Double>> tempHashMap = firstStar.tripleWithStats.stream().collect(Collectors.toMap(tws -> tws.triple, tws -> new Tuple4<>(tws.triple, tws.cardinality, tws.distinctSubjects, tws.distinctObjects), (a, b) -> b, HashMap::new));
        //adjust the remaining and processed lists members according to the triples of the first star
        for (int i = 0; i < tuple4List.size(); i++) {
            if (tempHashMap.containsValue(tuple4List.get(i))) {
                processed.put(i, tuple4List.get(i));
                queue.add(i);
            } else {
                remaining.put(i, tuple4List.get(i));
            }
        }

        double globalCost = firstStar.starCardinality;
        double sumGlobalCost = 0;
        for (int pointer = 0; pointer < tuple4List.size() - firstStar.tripleWithStats.size(); pointer++) {
            int i;
            Tuple3<Integer, Tuple4<Triple, Double, Double, Double>, Double> tripleWithCostAndIndex = null;
            Queue<Integer> localQueue = new LinkedList<>(queue);

            while (localQueue.size() != 0) {
                System.out.println(localQueue.size());
                Tuple3<Integer, Tuple4<Triple, Double, Double, Double>, Double> local = computeCost(localQueue.poll(), remaining, processed, globalCost);
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
            System.out.println("\n >> Selected Triple: " + processed.get(i).toSpecialFormat() + "\t -->: Global Cost: " + globalCost + "\n");
            qpt.put(processed.get(i)._1, String.valueOf(globalCost));
            //adding processed triple to queue
            //System.out.println("Global Cost for this iteration: " + globalCost);
            queue.add(i);
            remaining.remove(i);
            sumGlobalCost += globalCost;
        }
        System.out.println("\n >> Total Cost: " + sumGlobalCost);
        BasicPattern bp = new BasicPattern();
        for (Integer integer : queue) {
            //System.out.println(processed.get(integer));
            bp.add(processed.get(integer)._1);
        }
        //System.out.println(bp);
        return queryUtils.makeQueryAlgebra(query, bp, projectedVariables);
    }

    private Tuple3<Integer, Tuple4<Triple, Double, Double, Double>, Double> computeCost(int pointer, HashMap<Integer, Tuple4<Triple, Double, Double, Double>> remaining, HashMap<Integer, Tuple4<Triple, Double, Double, Double>> processed, Double globalCost) {
        Tuple4<Triple, Double, Double, Double> tripleA = processed.get(pointer);
        //System.out.println(pointer + " --> " + remaining.size() + " --> " + processed.size());

        double minCost = -1;
        //String order = "";
        int minKey = 0;

        double tripleACost = globalCost;
        //double tripleACost = tripleA._2;

        for (Map.Entry<Integer, Tuple4<Triple, Double, Double, Double>> entry : remaining.entrySet()) {
            Integer key = entry.getKey();
            Tuple4<Triple, Double, Double, Double> tripleB = entry.getValue();

            // Print in this format:  tripleA ⨝ Triple B
            queryUtils.prettyPrint(tripleA._1);
            System.out.print(" " + new String(Character.toChars(0x2A1D)) + " ");
            queryUtils.prettyPrint(tripleB._1);
            int actualJoinCardinality = 0;

            double currentCost = 0;
            boolean joinFlag = false;
            //Find type of join between Triple A and Triple B
            if (tripleA._1.getSubject().equals(tripleB._1.getSubject())) {
                //System.out.println(tripleA._1.toString() + " SS_JOIN " + tripleB._1.toString());
                currentCost = estimateJoinCardinality(tripleACost, tripleB._2, tripleA._3, tripleB._3);
                joinFlag = true;
            }
            if (tripleA._1.getSubject().toString().equals(tripleB._1.getObject().toString())) {
                //System.out.println(tripleA._1.toString() + " SO_JOIN " + tripleB._1.toString());
                currentCost = estimateJoinCardinality(tripleACost, tripleB._2, tripleA._3, tripleB._4);

                joinFlag = true;
            }
            if (tripleA._1.getObject().toString().equals(tripleB._1.getSubject().toString())) {
                //System.out.println(tripleA._1.toString() + " OS_JOIN " + tripleB._1.toString());
                currentCost = estimateJoinCardinality(tripleACost, tripleB._2, tripleA._4, tripleB._3);

                joinFlag = true;
            }
            if (tripleA._1.getObject().toString().equals(tripleB._1.getObject().toString())) {
                //System.out.println(tripleA._1.toString() + " OO_JOIN " + tripleB._1.toString());

                //an extra check to handle this kind of case where there will be a cartesian product
                // ?x a ub:GraduateStudent.
                // ?y a ub:GraduateStudent.

                if (!tripleA._1.getSubject().toString().equals(tripleB._1.getSubject().toString()) &&
                        tripleA._1.getPredicate().toString().equals(tripleB._1.getPredicate().toString()) &&
                        tripleA._1.getPredicate().toString().equals(RDF.type.toString())) {
                    joinFlag = false;
                    //System.out.print(" ** CP");
                } else {
                    currentCost = estimateJoinCardinality(tripleACost, tripleB._2, tripleA._4, tripleB._4);
                    joinFlag = true;
                }

            }
            if (joinFlag && extendedLogs) {
                List<Triple> t = new ArrayList<>();
                t.add(tripleA._1);
                t.add(tripleB._1);
                actualJoinCardinality = executeCountQueryOnGraphDB(buildCountQuery(t));
            }

            // in case no join variable exists between/among the variables of tripleA and tripleB, then its CP
            if (!joinFlag) {
                //System.out.print(" CP");
                currentCost = tripleACost * tripleB._2;
                System.out.print("-- " + tripleACost + " * " + tripleB._2 + " --");
                if (extendedLogs)
                    actualJoinCardinality = executeCountQueryOnGraphDB(buildCountQuery(new ArrayList<Triple>(Arrays.asList(tripleA._1)))) * executeCountQueryOnGraphDB(buildCountQuery(new ArrayList<Triple>(Arrays.asList(tripleB._1))));
            }


            if (minCost == -1 || minCost > currentCost) {
                minCost = currentCost;
                minKey = key;
                //order = pointer + "." + key + " = " + tripleA._1 + " --> " + tripleB._1;
            }
            System.out.print("> E: " + format(currentCost) + " --: A:" + format((double) actualJoinCardinality) + "\n");
        }

        return new Tuple3<>(minKey, remaining.get(minKey), minCost);
    }

    private double estimateJoinCardinality(double cardA, double cardB, double v1, double v2) {
        double nom = cardA * cardB;
        /*if (v1 > cardA)
            v1 = cardA;

        if (v2 > cardB)
            v2 = cardB;*/
        System.out.print(" --( " + cardA + " * " + cardB + " / " + "Max (" + v1 + " , " + v2 + ")" + " )--");

        double den = Math.max(v1, v2);
        return nom / den;
    }

    private void manageEstimatesForStarTPsForLogging(Star firstStar) {
        if (firstStar.tripleWithStats.size() == 1) {
            qpt.put(firstStar.tripleWithStats.get(0).triple, String.valueOf(firstStar.tripleWithStats.get(0).cardinality));
        } else if (firstStar.tripleWithStats.size() == 2) {
            qpt.put(firstStar.tripleWithStats.get(0).triple, String.valueOf(firstStar.tripleWithStats.get(0).cardinality));
            qpt.put(firstStar.tripleWithStats.get(1).triple, String.valueOf(firstStar.getStarCardinality()));
        } else if (firstStar.tripleWithStats.size() > 2) {
            List<TripleWithStats> listTws = new ArrayList<>();

            int tracker = 0;
            for (TripleWithStats tws : firstStar.tripleWithStats) {
                if (tracker == 0) {
                    listTws.add(tws);
                    qpt.put(tws.triple, String.valueOf(tws.cardinality));
                } else {
                    listTws.add(tws);
                    qpt.put(tws.triple, String.valueOf(StarsEvaluator.getEstimatedStarCardinality(listTws)));
                }
                tracker++;
            }
        }
    }

    private void logEstimatedAndActualJoinCardinalityOfQueryPlan(String queryId) {
        String queryPlanFileName = queryId + "_plan.txt";
        // Log estimated join cardinality in file using qpt current values
        logQueryPlanTriplesToFile(queryPlanFileName, queryId + "\n -- Estimated Only -----\n");

        // Log actual join cardinality by obtaining the actual join cardinality
        getActualJoinCardinalityViaQuery();
        logQueryPlanTriplesToFile(queryPlanFileName, queryId + "\n\n -- Estimated and Actual -----\n");
    }

    /**
     * In this method we build and execute count(*) query from each next triple pattern join to get the real join cardinality
     */
    private void getActualJoinCardinalityViaQuery() {
        List<Triple> tripleList = new ArrayList<>();
        qpt.forEach((k, v) -> {
            //System.out.println(k + " -> " + v);
            tripleList.add(k);
            int count = executeCountQueryOnGraphDB(buildCountQuery(tripleList));
            qpt.replace(k, v, "E: " + format(Double.parseDouble(v)) + " -- A:" + format((double) count));
        });
    }

    public Integer executeCountQueryOnGraphDB(String query) {
        int i = -1;
        try {
            List<BindingSet> result = graphDBUtils.runSelectQueryWithTimeOut(query);
            if (result.size() > 0)
                i = Integer.parseInt(result.get(0).getValue("Count").stringValue());
        } catch (Exception e) {
            System.out.println("ERROR: GraphDB Query interruption or error: ");
            e.printStackTrace();
        }
        return i;
    }

    private void logQueryPlanTriplesToFile(String queryId, String message) {
        Starter.writeToFile(message + "\n", queryId, true);
        System.out.println(message);
        qpt.forEach((k, v) -> {
            Starter.writeToFile(k + " -> " + v + "\n", queryId, true);
            System.out.println(k + " -> " + v);
        });
    }

    public String buildCountQuery(List<Triple> tripleList) {
        SelectBuilder selectBuilder = null;
        try {
            selectBuilder = new SelectBuilder()
                    .addVar("COUNT (*)", "?Count")
                    .addPrefix("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
                    .addPrefix("ub", "http://swat.cse.lehigh.edu/onto/univ-bench.owl#");
            tripleList.forEach(selectBuilder::addWhere);

        } catch (ParseException e) {
            e.printStackTrace();
        }
        assert selectBuilder != null;
        return selectBuilder.buildString();
    }

    //Getter Functions
    List<String> getProjectedVariables() {
        return Lists.transform(projectedVariables, Functions.toStringFunction());
    }

    public List<Var> getVarProjectedVariables() {
        return projectedVariables;
    }

    public String format(Double n) {
        return NumberFormat.getNumberInstance(Locale.UK).format(n);
    }
}
