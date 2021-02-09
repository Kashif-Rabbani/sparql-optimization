package uk.ac.ox.krr.cardinality.eval;

import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.syntax.ElementFilter;
import org.apache.jena.sparql.syntax.ElementVisitorBase;

public class ElemVisitor extends ElementVisitorBase {
    public ElemVisitor() {
        super();
    }
    protected Expr filterExpression ;

    @Override
    public void visit(ElementFilter el) {
        filterExpression = el.getExpr();
    }

    public Expr getFilterExpression() {
        return filterExpression;
    }
}
