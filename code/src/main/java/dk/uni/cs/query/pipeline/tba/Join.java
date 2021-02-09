package dk.uni.cs.query.pipeline.tba;

import org.apache.jena.graph.Triple;

import java.util.HashMap;

public class Join {
     Triple tripleA;
     Triple tripleB;
    
    public  Triple getTripleA() {
        return tripleA;
    }
    
    public Triple getTripleB() {
        return tripleB;
    }
    
    @Override
    public String toString() {
        return tripleA + " ⨝ " + tripleB + "";
    }
    
    public Join(Triple t1, Triple t2) {
        tripleA = t1;
        tripleB = t2;
    }
    
}
