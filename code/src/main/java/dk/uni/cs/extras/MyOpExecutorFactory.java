package dk.uni.cs.extras;

import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;

public class MyOpExecutorFactory implements OpExecutorFactory {
    public MyOpExecutorFactory() {
    }

    @Override
    public OpExecutor create(ExecutionContext execCxt) {
        return null;
    }
}
