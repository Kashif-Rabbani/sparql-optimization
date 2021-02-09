package dk.uni.cs.query.pipeline.sba;


import org.apache.jena.graph.Triple;
import org.apache.jena.vocabulary.RDF;

import java.text.DecimalFormat;

public class TripleWithStats {
    public double cardinality;
    public double distinctSubjects;
    public double distinctObjects;
    public Triple triple;
    public boolean isRdfType = false;
    
    public boolean isRdfType() {
        return isRdfType;
    }
    
    public double getCardinality() {
        return cardinality;
    }
    
    public double getDistinctSubjects() {
        return distinctSubjects;
    }
    
    public double getDistinctObjects() {
        return distinctObjects;
    }
    
    public Triple getTriple() {
        return triple;
    }
    
    
    @Override
    public String toString() {
        DecimalFormat formatter = new DecimalFormat("#,###.00");
        return "\n" +
                ", Triple =" + triple +
                ", Card=" + formatter.format(cardinality) +
                ", DSC=" + distinctSubjects +
                ", DOC=" + distinctObjects +
                "";
    }
    
    public TripleWithStats(double cardinality, double distinctSubjects, double distinctObjects, Triple triple) {
        this.cardinality = cardinality;
        this.distinctSubjects = distinctSubjects;
        this.distinctObjects = distinctObjects;
        this.triple = triple;
        if (this.triple.getPredicate().toString().equals(RDF.type.toString())) {
            isRdfType = true;
        }
    }
}
