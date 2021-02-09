package dk.uni.cs.query.pipeline.tba;

import dk.uni.cs.query.pipeline.sba.Star;
import dk.uni.cs.utils.Tuple4;
import org.apache.jena.graph.Triple;

public class TheTriple {
    public int index;
    public Triple triple;
    public Boolean rdfType;
    public Star star;
    public Tuple4<Triple, Double, Double, Double> stats;
    
    @Override
    public String toString() {
        return "TheTriple{" +
                "index=" + index +
                ", triple=" + triple +
                ", rdfType=" + rdfType +
                ", star=" + star +
                ", stats=" + stats +
                '}';
    }
    
    public int getIndex() {
        return index;
    }
    
    public TheTriple() {}
    
    public TheTriple(int index, Triple triple, Boolean rdfType, Star star, Tuple4<Triple, Double, Double, Double> stats) {
        this.index = index;
        this.triple = triple;
        this.rdfType = rdfType;
        this.star = star;
        this.stats = stats;
    }
    
    public Tuple4<Triple, Double, Double, Double> getStats() {
        return stats;
    }
    
    public void setStats(Tuple4<Triple, Double, Double, Double> stats) {
        this.stats = stats;
    }
    
    
}
