package dk.uni.cs.query.pipeline;

import dk.uni.cs.extras.BGPVisitor;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.AlgebraGenerator;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.OpWalker;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.core.BasicPattern;

import java.util.*;


public class TriplesShuffler {
    private Query query;
    private String queryId;
    private List<Query> queries = new ArrayList<>();
    
    public TriplesShuffler() {
    }
    
    public TriplesShuffler(String query) {
        this.query = QueryFactory.create(query);
    }
    
    public TriplesShuffler(Query query, String queryId) {
        this.query = query;
        this.queryId = queryId;
    }
    
    public String shuffleSingleQueryTPs() {
        List<Triple> triples = extractBGPTriples(this.query);
        return (constructQueryFromBGPTriples(shuffleQueryTPs(triples), this.query)).toString();
    }
    
    public void run() {
        List<Triple> triples = extractBGPTriples(this.query);
        for (int i = 0; i < 10; i++) {
            queries.add(constructQueryFromBGPTriples(shuffleQueryTPs(triples), this.query));
        }
        //executeQueriesOnJena();
        //executeQueriesOnVirtuoso();
    }
    
    
    public List<Triple> extractBGPTriples(Query query) {
        ArrayList<HashSet<Triple>> bgps = new ArrayList<>();
        Op op = (new AlgebraGenerator()).compile(query);
        BGPVisitor bgpVisitor = new BGPVisitor();
        OpWalker.walk(op, bgpVisitor);
        bgps = bgpVisitor.getBGPs();
        return new ArrayList<>(bgps.get(0));
    }
    
    public List<Triple> shuffleQueryTPs(List<Triple> triples) {
        Collections.shuffle(triples, new Random());
        return triples;
    }
    
    public Query constructQueryFromBGPTriples(List<Triple> triples, Query query) {
        Op op;
        BasicPattern pat = new BasicPattern();
        for (Triple triple : triples) {
            pat.add(triple);
        }
        
        op = new OpBGP(pat);
        if (!query.isQueryResultStar()) {
            op = new OpProject(op, query.getProject().getVars());
        }
        Query q = OpAsQuery.asQuery(op);
        q.setPrefixMapping(query.getPrefixMapping());
        q.setQuerySelectType();
        return q;
    }
}



/*    private void executeQueriesOnJena() {
        Dataset ds = RdfUtils.getTDBDataset();
        Model gm = ds.getNamedModel(Main.getRdfModelIRI());
        ds.begin(ReadWrite.READ);
        System.out.println("Executing Queries on Jena");
        long totalPlanningTime = 0, totalExecutionTime = 0, totalTime = 0;
        List<Query> queryList = this.queries;
        for (int i = 0; i < queryList.size(); i++) {
            Query q = queryList.get(i);

            long beginPlanning = System.currentTimeMillis();
            QueryExecution qExec = QueryExecutionFactory.create(QueryFactory.create(q), gm);
            qExec.getContext().set(ARQ.symLogExec, Explain.InfoLevel.ALL);
            ResultSet resultJenaExec = qExec.execSelect();
            long endPlanning = System.currentTimeMillis() - beginPlanning;

            long beginExecution = System.currentTimeMillis();
            int resultsSetSize = new BenchmarkQuery().iterateOverResultsWithTimeOut(resultJenaExec);
            long endExecution = System.currentTimeMillis() - beginExecution;

            totalExecutionTime += endExecution;
            totalPlanningTime += endPlanning;
            totalTime += (endPlanning + endExecution);

            logger.info(" ------> " + queryId + "_" + i + "_" + " Plan : " + endPlanning + " ms" +
                    " , Execution : " + endExecution + " ms , " + " Total: " + (endPlanning + endExecution) + " , ResultSize: " + resultsSetSize);
            qExec.close();
        }

        logger.info(" ------> " + queryId + " --> Average Planning: " + totalPlanningTime / queryList.size() +
                ", Average Execution: " + totalExecutionTime / queryList.size() +
                ", Total Average Time: " + totalTime / queryList.size()
        );

        if (ds.isInTransaction()) {
            ds.end();
            ds.close();
        }
    }

    private void executeQueriesOnVirtuoso() {

        // Connect to SPARQL
        VirtuosoConnector conn = null;
        try {
            conn = new VirtuosoConnector(ConfigManager.getProperty("virtuosoHost"), ConfigManager.getProperty("virtuosoUser"), ConfigManager.getProperty("virtuosoPw"));
        } catch (Exception ex) {
            System.err.println("Could not connect to Virtuoso SPARQL server: " + ex.getMessage());
            System.exit(2);
        }

        List<Query> queryList = this.queries;
        System.out.println("Executing Queries on Virtuoso");
        long totalPlanningTime = 0, totalExecutionTime = 0, totalTime = 0;

        for (int i = 0; i < queryList.size(); i++) {
            Query q = queryList.get(i);
            long beginPlanning = System.currentTimeMillis();
            ResultSet resultSet = conn.execSelect(q.toString());
            long endPlanning = System.currentTimeMillis() - beginPlanning;

            long beginExecution = System.currentTimeMillis();
            int resultsSetSize = new BenchmarkQuery().iterateOverResultsWithTimeOut(resultSet);
            long endExecution = System.currentTimeMillis() - beginExecution;
            totalExecutionTime += endExecution;
            totalPlanningTime += endPlanning;
            totalTime += (endPlanning + endExecution);
            logger.info(" ------> " + queryId + "_" + i + "_" + " Plan : " + endPlanning + " ms" +
                    " , Execution : " + endExecution + " ms , " + " Total: " + (endPlanning + endExecution) + " , ResultSize: " + resultsSetSize);
        }
        logger.info(" ------> " + queryId + " --> Average Planning: " + totalPlanningTime / queryList.size() +
                ", Average Execution: " + totalExecutionTime / queryList.size() +
                ", Total Average Time: " + totalTime / queryList.size()
        );
    }*/