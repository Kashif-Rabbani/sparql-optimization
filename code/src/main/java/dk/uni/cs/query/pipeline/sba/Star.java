package dk.uni.cs.query.pipeline.sba;

import org.apache.jena.graph.Triple;
import org.apache.jena.vocabulary.RDF;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class Star {
    public double starCardinality;
    public double starWeight;
    public Triple leastCardinalityTriple;
    public double valueOfLeastCard;
    public boolean isLeastCardTripleIsOfRdfType;
    public boolean isTypeDefined;
    public Triple rdfTypeTriple;
    public List<TripleWithStats> tripleWithStats;
    public List<String> starTriplesVars;
    public List<Triple> triples = new ArrayList<>();
  
    
    public Star(double starCardinality, List<TripleWithStats> t) {
        TripleWithStats tws = t.stream().min(Comparator.comparing(TripleWithStats::getCardinality)).get();
        this.leastCardinalityTriple = tws.triple;
        this.valueOfLeastCard = tws.cardinality;
    
        t.forEach(tObj -> {
            if (tObj.isRdfType && !tObj.triple.getObject().isVariable()) {
                this.isTypeDefined = true;
                rdfTypeTriple = tObj.triple;
            }
            triples.add(tObj.getTriple());
        });
        this.isLeastCardTripleIsOfRdfType = this.leastCardinalityTriple.getPredicate().toString().equals(RDF.type.toString());
        this.starCardinality = starCardinality;
        this.tripleWithStats = t;
        extractVariablesFromStarTriples();
    }
    
    
    private void extractVariablesFromStarTriples() {
        starTriplesVars = new ArrayList<>();
        for (TripleWithStats tObj : tripleWithStats) {
            starTriplesVars.addAll(getVarsOfTriple(tObj.triple));
        }
    }
    
    private List<String> getVarsOfTriple(Triple t) {
        List<String> varsOfCurrentTriple = new ArrayList<>();
        if (t.getSubject().isVariable()) {
            varsOfCurrentTriple.add(t.getSubject().getName());
        }
        if (t.getObject().isVariable()) {
            varsOfCurrentTriple.add(t.getObject().getName());
        }
        return varsOfCurrentTriple;
    }
    
    
    public double getValueOfLeastCard() {
        return valueOfLeastCard;
    }
    
    public double getStarCardinality() {
        return starCardinality;
    }
    
    public Triple getRdfTypeTriple() {
        return rdfTypeTriple;
    }
    
    public List<String> getStarTriplesVars() {
        return starTriplesVars;
    }
    
    @Override
    public String toString() {
        return "\nStar\n{" +
                ", " + tripleWithStats +
                "} + --> " + starCardinality;
    }
}

