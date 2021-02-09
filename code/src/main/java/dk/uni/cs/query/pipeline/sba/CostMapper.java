package dk.uni.cs.query.pipeline.sba;

import java.util.HashMap;
import java.util.List;

class CostMapper {
    List<Star> starList;
    
    String decider(HashMap<String, Double> hashMap, List<Star> starList) {
        this.starList = starList;
        double cost, cost1, cost2;
        String order = "";
        
        // 4 stars
        if (hashMap.size() == 6) {
            
            cost = costComparator(hashMap.get("1.2"), hashMap.get("3.4"));
            order = func("1.2", hashMap.get("1.2"), "3.4", hashMap.get("3.4"));
            
            cost1 = costComparator(hashMap.get("1.3"), hashMap.get("2.4"));
            if (cost1 < cost) {
                cost = cost1;
                order = func("1.3", hashMap.get("1.3"), "2.4", hashMap.get("2.4"));
            }
            
            cost2 = costComparator(hashMap.get("2.3"), hashMap.get("1.4"));
            if (cost2 < cost) {
                //cost = cost2;
                order = func("2.3", hashMap.get("2.3"), "1.4", hashMap.get("1.4"));
            }
        }
        
        
        // 3 stars
        if (hashMap.size() == 3) {
            //System.out.println("size 3");
            //System.out.println(hashMap);
            cost = hashMap.get("1.2");
            
            if (starList.get(0).starCardinality < starList.get(1).starCardinality) {
                order = "1.2.3";
            } else {
                order = "2.1.3";
            }
            
            if (hashMap.get("1.3") < cost) {
                cost = hashMap.get("1.3");
                if (starList.get(0).starCardinality < starList.get(2).starCardinality) {
                    order = "1.3.2";
                } else {
                    order = "3.1.2";
                }
                //order = "1.3.2";
            }
            
            if (hashMap.get("2.3") < cost) {
                cost = hashMap.get("2.3");
                
                if (starList.get(1).starCardinality < starList.get(2).starCardinality) {
                    order = "2.3.1";
                } else {
                    order = "3.2.1";
                }
                //order = "2.3.1";
            }
        }
        
        return order;
    }
    
    private String func(String pairA, Double costPairA, String pairB, Double costPairB) {
        //this.globalCost = costPairA * costPairB;
        String order = "";
        
        //Pair A Cost Based Manipulation
        if (starList.get(Integer.parseInt(pairA.split("\\.")[0]) - 1).starCardinality > starList.get(Integer.parseInt(pairA.split("\\.")[1]) - 1).starCardinality) {
            pairA = pairA.split("\\.")[1] + "." + pairA.split("\\.")[0];
        }
        //Pair B Cost based Manipulation
        if (starList.get(Integer.parseInt(pairB.split("\\.")[0]) - 1).starCardinality > starList.get(Integer.parseInt(pairB.split("\\.")[1]) - 1).starCardinality) {
            pairB = pairB.split("\\.")[1] + "." + pairB.split("\\.")[0];
        }
        
        if (costPairA > costPairB) {
            order = pairB + "." + pairA;
        } else {
            order = pairA + "." + pairB;
        }
        return order;
    }
    
    private static Double costComparator(Double v1, Double v2) {
        return v1 * v2;
    }
    
    
}
