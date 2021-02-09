package uk.ac.ox.krr.cardinality.eval;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpWalker;
import org.apache.jena.sparql.core.Var;

import java.io.File;
import java.util.List;

/* Class that provides access to a query triple patterns */
public class BGP {

    List<Triple> body;
    PrefixMapping p;
    List<Var> projectedVariables;
    boolean distinct;

    public BGP(String fileName) {

        this(new File(fileName));
    }

    public BGP(File f) {

        String fn = f.getAbsolutePath();
        Query query = QueryFactory.read(fn);
        //int begin = fn.lastIndexOf("/")+1;
        //int end = fn.lastIndexOf(".");
        //end = end == -1 ? fn.length() : end;
        //fn = fn.substring(begin, end);
        BGPInit(query);
    }

    public BGP(Query q) {

        BGPInit(q);
    }

    private void BGPInit(Query query) {
        p = query.getPrefixMapping();
        List<? extends Node> projectedVars = query.getProjectVars();
        //this.head = new Head(fn, projectedVars);
        Op op = Algebra.compile(query);
        TriplesVisitor mv = new TriplesVisitor();
        OpWalker ow = new OpWalker();
        ow.walk(op, mv);
        this.body = mv.getTriples();
        this.distinct = query.isDistinct();
        this.projectedVariables = query.getProjectVars();
    }

    public PrefixMapping getPrefixMapping() {

        return p;
    }

    public int getNumberTriples() {
        return body.size();
    }

    public List<Triple> getBody() {
        return this.body;
    }

    public List<Var> getProjectedVariables() {

        return projectedVariables;
    }

    public boolean getDistinct() {
        return distinct;
    }

    public String toString() {

        String s = "";
        for (Triple t : body) {
            s = s + t.toString()+". ";
        }
        if (body.size() > 0) {
            s = s.substring(0, s.length()-2);
        }
        return s;
    }
}
