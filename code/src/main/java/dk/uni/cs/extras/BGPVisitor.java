package dk.uni.cs.extras;

import org.apache.jena.sparql.algebra.OpVisitorBase;
import java.util.*;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.algebra.OpWalker;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.AlgebraGenerator;
import org.apache.jena.sparql.algebra.Op;

public class BGPVisitor extends OpVisitorBase {

        protected ArrayList<HashSet<Triple>> bgps = new ArrayList<HashSet<Triple>>();

        public BGPVisitor() {
                super();
        }

        public ArrayList<HashSet<Triple>> getBGPs() {
                return bgps;
        }

        @Override
        public void visit(OpUnion union) {
                super.visit(union);
        }

        @Override
        public void visit(OpLeftJoin leftjoin) {
                super.visit(leftjoin);
        }

        @Override
        public void visit(OpFilter filter) {
                super.visit(filter);
        }

        @Override
        public void visit(OpService service) {
                super.visit(service);
        }

        @Override
        public void visit(OpReduced reduced) {
                super.visit(reduced);
        }

        @Override
        public void visit(OpJoin node) {
            super.visit(node);
        }

        public void visit(OpBGP node) {
            HashSet<Triple> es = new HashSet<Triple>();
            BasicPattern bp = node.getPattern();
            Iterator<Triple> it = bp.iterator();
            while (it.hasNext()) {
                Triple t = it.next();
                es.add(t);
            }
            bgps.add(es);
        }
        @Override
        public void visit(OpTriple node) {
                //stmts.add(node);
        }

    public static void main (String[] args) {

        String queryIn = args[0];
        try {
            Query q = QueryFactory.read(queryIn);
            Op op = (new AlgebraGenerator()).compile(q);
            BGPVisitor bgpv = new BGPVisitor();
            OpWalker.walk(op, bgpv);
            ArrayList<HashSet<Triple>> bgps = bgpv.getBGPs();
            for (HashSet<Triple> bgp : bgps) {
                System.out.print(bgp);
            }
        } catch (org.apache.jena.query.QueryParseException e) {
            System.out.println("error while reading "+queryIn);
        }
    }
}

