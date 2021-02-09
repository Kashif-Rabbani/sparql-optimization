package uk.ac.ox.krr.cardinality.eval;//package semLAV;

import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.core.BasicPattern;

import java.util.ArrayList;
import java.util.List;

public class TriplesVisitor extends OpVisitorBase {

    List<Triple> elems;

    public TriplesVisitor() {
        super();
        elems = new ArrayList<Triple>();
    }

    public void visit(OpBGP opBGP) {

        BasicPattern bp = opBGP.getPattern();
        List<Triple> aux = bp.getList();
        elems.addAll(aux);
    }


    public List<Triple> getTriples() {
        return elems;
    }
}
