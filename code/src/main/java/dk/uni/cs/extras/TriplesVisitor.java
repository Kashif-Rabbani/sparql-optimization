package dk.uni.cs.extras;//package semLAV;

import org.apache.jena.sparql.algebra.OpVisitorBase;
import java.util.*;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.core.BasicPattern;

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
