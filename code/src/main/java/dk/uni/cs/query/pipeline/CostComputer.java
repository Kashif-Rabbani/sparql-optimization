package dk.uni.cs.query.pipeline;

import dk.uni.cs.query.pipeline.tba.Join;
import dk.uni.cs.utils.Log;
import dk.uni.cs.utils.Tuple3;
import dk.uni.cs.utils.Tuple4;
import org.apache.jena.graph.Triple;
import org.apache.jena.vocabulary.RDF;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

public class CostComputer {
    Tuple3<Integer, Tuple4<Triple, Double, Double, Double>, Double> cost = null;
    HashMap<Join, Double> joinCost = null;
    DecimalFormat formatter = new DecimalFormat("#,###.00");
    
    public HashMap<Join, Double> getJoinCost() {
        return joinCost;
    }
    
    public CostComputer() { }
    
    public Tuple3<Integer, Tuple4<Triple, Double, Double, Double>, Double> computeCost(int pointer, HashMap<Integer, Tuple4<Triple, Double, Double, Double>> remaining, HashMap<Integer, Tuple4<Triple, Double, Double, Double>> processed, Double globalCost) {
        joinCost = new HashMap<>();
        Tuple4<Triple, Double, Double, Double> tripleA = processed.get(pointer);
        
        Log.logLine(pointer + " --> " + remaining.size() + " --> " + processed.size());
        
        double minCost = -1;
        //String order = "";
        int minKey = 0;
        
        double tripleACost = globalCost;
        //double tripleACost = tripleA._2;
        
        for (Map.Entry<Integer, Tuple4<Triple, Double, Double, Double>> entry : remaining.entrySet()) {
            Integer key = entry.getKey();
            Tuple4<Triple, Double, Double, Double> tripleB = entry.getValue();
            
            Log.log(new QueryUtils().prettyPrint(tripleA._1));
            Log.log(" " + new String(Character.toChars(0x2A1D)) + " ");
            Log.log(new QueryUtils().prettyPrint(tripleB._1));
            
            Join j = new Join(tripleA._1, tripleB._1);
            
            double currentCost = 0;
            boolean joinFlag = false;
            //Find type of join between Triple A and Triple B
            if (tripleA._1.getSubject().equals(tripleB._1.getSubject())) {
                //Log.logLine(tripleA._1.toString() + " SS_JOIN " + tripleB._1.toString());
                currentCost = estimateJoinCardinality(tripleACost, tripleB._2, tripleA._3, tripleB._3);
                joinFlag = true;
            }
            if (tripleA._1.getSubject().toString().equals(tripleB._1.getObject().toString())) {
                //Log.logLine(tripleA._1.toString() + " SO_JOIN " + tripleB._1.toString());
                currentCost = estimateJoinCardinality(tripleACost, tripleB._2, tripleA._3, tripleB._4);
                
                joinFlag = true;
            }
            if (tripleA._1.getObject().toString().equals(tripleB._1.getSubject().toString())) {
                //Log.logLine(tripleA._1.toString() + " OS_JOIN " + tripleB._1.toString());
                currentCost = estimateJoinCardinality(tripleACost, tripleB._2, tripleA._4, tripleB._3);
                
                joinFlag = true;
            }
            if (tripleA._1.getObject().toString().equals(tripleB._1.getObject().toString())) {
                //Log.logLine(tripleA._1.toString() + " OO_JOIN " + tripleB._1.toString());
                
                //an extra check to handle this kind of case where there will be a cartesian product
                // ?x a ub:GraduateStudent.
                // ?y a ub:GraduateStudent.
                
                if (!tripleA._1.getSubject().toString().equals(tripleB._1.getSubject().toString()) &&
                        tripleA._1.getPredicate().toString().equals(tripleB._1.getPredicate().toString()) &&
                        tripleA._1.getPredicate().toString().equals(RDF.type.toString())) {
                    joinFlag = false;
                    //Log.log(" ** CP");
                } else {
                    currentCost = estimateJoinCardinality(tripleACost, tripleB._2, tripleA._4, tripleB._4);
                    joinFlag = true;
                }
                
            }
            // in case no join variable exists between/among the variables of tripleA and tripleB, then its CP
            if (!joinFlag) {
                //Log.log(" CP");
                currentCost = tripleACost * tripleB._2;
                //Log.log("-- " + tripleACost + " * " + tripleB._2 + " --");
            }
            
            if (minCost == -1 || minCost > currentCost) {
                minCost = currentCost;
                minKey = key;
                //order = pointer + "." + key + " = " + tripleA._1 + " --> " + tripleB._1;
            }
            Log.log(" --> " + formatter.format(currentCost) + " ______" + minCost + "\n");
            joinCost.put(j, currentCost);
        }
        
        return new Tuple3<>(minKey, remaining.get(minKey), minCost);
    }
    
    
    public double estimateJoinCostForTwoTriplePatterns(Tuple4<Triple, Double, Double, Double> tripleA, Tuple4<Triple, Double, Double, Double> tripleB, double tripleACost) {
        double currentCost = 0;
        Log.log(new QueryUtils().prettyPrint(tripleA._1));
        Log.log(" " + new String(Character.toChars(0x2A1D)) + " ");
        Log.log(new QueryUtils().prettyPrint(tripleB._1));
    
        boolean joinFlag = false;
        //Find type of join between Triple A and Triple B
        if (tripleA._1.getSubject().equals(tripleB._1.getSubject())) {
            //Log.logLine(tripleA._1.toString() + " SS_JOIN " + tripleB._1.toString());
            currentCost = estimateJoinCardinality(tripleACost, tripleB._2, tripleA._3, tripleB._3);
            joinFlag = true;
        }
        if (tripleA._1.getSubject().toString().equals(tripleB._1.getObject().toString())) {
            //Log.logLine(tripleA._1.toString() + " SO_JOIN " + tripleB._1.toString());
            currentCost = estimateJoinCardinality(tripleACost, tripleB._2, tripleA._3, tripleB._4);
            
            joinFlag = true;
        }
        if (tripleA._1.getObject().toString().equals(tripleB._1.getSubject().toString())) {
            //Log.logLine(tripleA._1.toString() + " OS_JOIN " + tripleB._1.toString());
            currentCost = estimateJoinCardinality(tripleACost, tripleB._2, tripleA._4, tripleB._3);
            
            joinFlag = true;
        }
        if (tripleA._1.getObject().toString().equals(tripleB._1.getObject().toString())) {
            //Log.logLine(tripleA._1.toString() + " OO_JOIN " + tripleB._1.toString());
            
            //an extra check to handle this kind of case where there will be a cartesian product
            // ?x a ub:GraduateStudent.
            // ?y a ub:GraduateStudent.
            
            if (!tripleA._1.getSubject().toString().equals(tripleB._1.getSubject().toString()) &&
                    tripleA._1.getPredicate().toString().equals(tripleB._1.getPredicate().toString()) &&
                    tripleA._1.getPredicate().toString().equals(RDF.type.toString())) {
                joinFlag = false;
                //Log.log(" ** CP");
            } else {
                currentCost = estimateJoinCardinality(tripleACost, tripleB._2, tripleA._4, tripleB._4);
                joinFlag = true;
            }
            
        }
        // in case no join variable exists between/among the variables of tripleA and tripleB, then its CP
        if (!joinFlag) {
            //Log.log(" CP");
            currentCost = tripleACost * tripleB._2;
            //Log.log("-- " + tripleACost + " * " + tripleB._2 + " --");
        }
        System.out.println("Current Cost " + currentCost);
        return currentCost;
    }
    
    double estimateJoinCardinality(double cardA, double cardB, double v1, double v2) {
        double nom = cardA * cardB;
        /*if (v1 > cardA)
            v1 = cardA;

        if (v2 > cardB)
            v2 = cardB;*/
        Log.log(" --( " + cardA + " * " + cardB + " / " + "Max (" + v1 + " , " + v2 + ")" + " )--");
        
        double den = Math.max(v1, v2);
        return nom / den;
    }
}