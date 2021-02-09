package dk.uni.cs.extras;

import org.apache.jena.atlas.lib.Lib ;
import org.apache.jena.atlas.logging.Log ;
import org.apache.jena.sparql.core.BasicPattern ;
import org.apache.jena.sparql.core.Substitute ;
import org.apache.jena.sparql.engine.ExecutionContext ;
import org.apache.jena.sparql.engine.QueryIterator ;
import org.apache.jena.sparql.engine.binding.Binding ;
import org.apache.jena.sparql.engine.iterator.QueryIterBlockTriplesStar;
import org.apache.jena.sparql.engine.iterator.QueryIterPeek ;
import org.apache.jena.sparql.engine.main.StageGenerator;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderLib ;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderProc ;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderTransformation ;
import org.apache.jena.sparql.mgt.Explain ;

/**
 * Generic - always works - StageGenerator.
 * Uses the RDF* QueryIterBlockTriplesStar solver.
 */
public class MyStageGenerator implements StageGenerator {
    StageGenerator above = null ;

    public MyStageGenerator (StageGenerator original)
    { above = original ; }
    private static final ReorderTransformation reorderFixed = ReorderLib.fixed() ;

    @Override
    public QueryIterator execute(BasicPattern pattern, QueryIterator input, ExecutionContext execCxt) {
        if ( input == null )
            Log.error(this, "Null input to " + Lib.classShortName(this.getClass())) ;

        // Choose reorder transformation and execution strategy.
        System.out.println("Here h");
        ReorderTransformation reorder = reorderFixed ;
        return execute(pattern, reorder, input, execCxt) ;
    }

    protected QueryIterator execute(BasicPattern pattern, ReorderTransformation reorder,
                                    QueryIterator input, ExecutionContext execCxt) {
        Explain.explain(pattern, execCxt.getContext()) ;
        System.out.println("Here");
        if ( ! input.hasNext() )
            return input ;

        if ( reorder != null && pattern.size() >= 2 ) {
            // If pattern size is 0 or 1, nothing to do.
            BasicPattern bgp2 = pattern ;

            // Try to ground the pattern
            if ( ! input.isJoinIdentity() ) {
                QueryIterPeek peek = QueryIterPeek.create(input, execCxt) ;
                // And now use this one
                input = peek ;
                Binding b = peek.peek() ;
                bgp2 = Substitute.substitute(pattern, b) ;
            }
            ReorderProc reorderProc = reorder.reorderIndexes(bgp2) ;
            pattern = reorderProc.reorder(pattern) ;
        }
        Explain.explain("Reorder/generic", pattern, execCxt.getContext()) ;
        return QueryIterBlockTriplesStar.create(input, pattern, execCxt) ;
    }
}
