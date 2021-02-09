package dk.uni.cs.query.pipeline;

import dk.uni.cs.shapes.Statistics;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

import java.util.*;

public class SubQuery {
    Collection<Triple> triples;
    Set<String> predicates = null;
    HashMap<String, Set<Triple>> map;
    Set<Triple> otherTriples;
    public boolean isSubjectStar;
    public boolean isTypeDefined;
    
    public Triple typeDefinedTriple;
    
    public Triple getTypeDefinedTriple() {
        return typeDefinedTriple;
    }
    
    public void setTypeDefinedTriple(Triple typeDefinedTriple) {
        this.typeDefinedTriple = typeDefinedTriple;
    }
    
    
    HashMap<Triple, List<Statistics>> relevantShapes = null;

    public SubQuery(Collection<Triple> ts) {
        triples = ts;
    }

    public void setSubjectStar() {
        isSubjectStar = true;
    }

    public boolean isTypeDefined() {
        return isTypeDefined;
    }

    public void setRdfTypeFlag(Boolean value) {
        isTypeDefined = value;
    }

    public String toString() {
        return triples.toString() + " " + isSubjectStar;
    }

    public boolean isSubjectStar() {
        return isSubjectStar;
    }

    public Collection<Triple> getTriples() {
        return triples;
    }

    public Set<String> getPredicates() {
        if (predicates == null) {
            predicates = new HashSet<String>();
            map = new HashMap<String, Set<Triple>>();
            otherTriples = new HashSet<Triple>();
            for (Triple t : triples) {
                Node p = t.getPredicate();
                if (p.isURI()) {
                    String pStr = "<" + p.getURI() + ">";
                    predicates.add(pStr);
                    Set<Triple> ts = map.get(pStr);
                    if (ts == null) {
                        ts = new HashSet<Triple>();
                        ts.add(t);
                        map.put(pStr, ts);
                    }
                } else {
                    otherTriples.add(t);
                }
            }
        }
        return predicates;
    }

    public Set<Triple> getTriples(String p) {
        return map.get(p);
    }

    public Set<Triple> getUnboundedPredicateTriples() {
        return otherTriples;
    }

    public void setShapes(HashMap<Triple, List<Statistics>> shapesOfStar) {
        this.relevantShapes = shapesOfStar;
    }

    public HashMap<Triple, List<Statistics>> getShapes() {
        return relevantShapes;
    }

}
