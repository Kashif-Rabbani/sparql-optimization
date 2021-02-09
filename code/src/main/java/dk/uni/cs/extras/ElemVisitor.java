package dk.uni.cs.extras;

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
