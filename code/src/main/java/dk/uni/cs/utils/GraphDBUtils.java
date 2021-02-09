package dk.uni.cs.utils;

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.ntriples.NTriplesWriter;
import org.eclipse.rdf4j.rio.turtle.TurtleWriter;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import static dk.uni.cs.utils.RdfUtils.logger;

public class GraphDBUtils {
    private final KBManagement kbManager = new KBManagement();
    private final Repository repository = kbManager.initGraphDBRepository();
    private final RepositoryConnection repositoryConnection = repository.getConnection();
    
    public ValueFactory getValueFactory() {
        return repositoryConnection.getValueFactory();
    }
    
    public List<BindingSet> runSelectQuery(String query) {
        List<BindingSet> result = new ArrayList<>();
        try {
            TupleQuery tupleQuery = repositoryConnection.prepareTupleQuery(QueryLanguage.SPARQL, query);
            TupleQueryResult classesQueryResult = tupleQuery.evaluate();
            classesQueryResult.forEach(result::add);
            classesQueryResult.close();
        } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
            if (repositoryConnection.isActive())
                repositoryConnection.rollback();
        }
        return result;
    }
    
    
    public void runGraphQuery(String query, String address) {
        try {
            GraphQuery graphQuery = repositoryConnection.prepareGraphQuery(QueryLanguage.SPARQL, query);
            
            GraphQueryResult graphQueryResult = graphQuery.evaluate();
            
            OutputStream out = new FileOutputStream(address, true);
            RDFWriter writer = new NTriplesWriter(out);
            graphQuery.evaluate(writer);
            
            graphQueryResult.close();
        } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
            if (repositoryConnection.isActive())
                repositoryConnection.rollback();
        }
    }
    
    
    public List<BindingSet> runSelectQueryWithTimeOut(String query) {
        List<BindingSet> result = new ArrayList<>();
        try {
            TupleQuery tupleQuery = repositoryConnection.prepareTupleQuery(QueryLanguage.SPARQL, query);
            tupleQuery.setMaxExecutionTime(300);
            TupleQueryResult classesQueryResult = tupleQuery.evaluate();
            classesQueryResult.forEach(result::add);
            classesQueryResult.close();
        } catch (Exception e) {
            logger.error(e.getMessage());
            //e.printStackTrace();
            if (repositoryConnection.isActive())
                repositoryConnection.rollback();
        }
        return result;
    }
    
    public void updateQueryExecutor(String query) {
        try {
            //System.out.println(query);
            repositoryConnection.begin();
            Update updateOperation = repositoryConnection.prepareUpdate(QueryLanguage.SPARQL, query);
            updateOperation.execute();
            repositoryConnection.commit();
            //repositoryConnection.close();
        } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
            if (repositoryConnection.isActive())
                repositoryConnection.rollback();
        }
    }
}
