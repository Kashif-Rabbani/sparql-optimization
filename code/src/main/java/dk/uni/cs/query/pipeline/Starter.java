package dk.uni.cs.query.pipeline;

import dk.uni.cs.Main;
import dk.uni.cs.utils.ConfigManager;
import dk.uni.cs.utils.QueryReader;
import dk.uni.cs.utils.RdfUtils;
import dk.uni.cs.virtuoso.VirtuosoConnector;
import org.apache.jena.ext.com.google.common.base.Functions;
import org.apache.jena.ext.com.google.common.collect.Lists;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.ResultSetStream;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.mgt.Explain;
import org.apache.jena.tdb.base.StorageException;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class Starter {
    final static org.apache.log4j.Logger logger = Logger.getLogger(Starter.class);
    
    String tuple = "";
    String plan = "";
    String execute = "";
    String resultSetSize = "";
    boolean timeOutFlagForCurrentQuery = false;
    public final boolean benchmarkShapeApproach = Boolean.parseBoolean(ConfigManager.getProperty("shapeExec"));
    public final boolean benchmarkJena = Boolean.parseBoolean(ConfigManager.getProperty("jenaExec"));
    public final boolean benchmarkVoidApproach = Boolean.parseBoolean(ConfigManager.getProperty("globalStatsExec"));
    public final boolean benchmarkQueryGivenOrder = Boolean.parseBoolean(ConfigManager.getProperty("executeQueryAsItIs"));
    public final boolean benchmarkQueryOtherPurpose = Boolean.parseBoolean(ConfigManager.getProperty("executeQueryForOtherPurpose"));
    private long queryTimeOutInMS = 300000; // 5 minutes
    
    Dataset ds = null;
    
    private VirtuosoConnector conn = null;
    
    public Starter() {
        readQueriesFromFolder();
    }
    
    public String getLogTuple() {
        return tuple;
    }
    
    public void readQueriesFromFolder() {
        HashMap<String, Query> queriesWithID = new QueryReader().getQueriesWithID();
        for (Map.Entry<String, Query> entry : queriesWithID.entrySet()) {
            String queryId = entry.getKey();
            Query query = entry.getValue();
            logger.info("\n________ " + queryId + " ________\n");
            System.out.println(query.toString());
            executeQuery(queryId, query.toString());
            //executeTheGivenQueryAsItIs(query.toString());
        }
    }
    
    public void executeQuery(String queryId, String query) {
        final int counter = Integer.parseInt(ConfigManager.getProperty("queryRunnerCounter"));
        
        ds = RdfUtils.getTDBDataset();
        Model gm = ds.getNamedModel(Main.getRdfModelIRI());
        if (!ds.isInTransaction())
            ds.begin(ReadWrite.READ);
        tuple += queryId + ",";
        this.plan = "";
        this.execute = "";
        this.resultSetSize = "";
        this.timeOutFlagForCurrentQuery = false;
        for (int i = 0; i < counter; i++) {
            // Shuffle the query triple patterns randomly
            TriplesShuffler ts = new TriplesShuffler();
            Query q = QueryFactory.create(query);
            Query shuffledQuery = ts.constructQueryFromBGPTriples(ts.shuffleQueryTPs(ts.extractBGPTriples(q)), q);
            
            // Construct plan and note planning time & Execute the query plan and note the execution time (within timout 5 min limit)
            if (benchmarkJena)
                benchmarkQueryInJena(gm, queryId, shuffledQuery.toString());
            
            if (benchmarkShapeApproach) {
                if (timeOutFlagForCurrentQuery) {
                    plan += "_";
                    execute += "_";
                    resultSetSize += "_";
                    break;
                } else {
                    benchmarkQueryInShapeApproach(gm, queryId, shuffledQuery.toString());
                }
            }
            // BaselineApproach with global statistics
            if (benchmarkVoidApproach) {
                if (timeOutFlagForCurrentQuery) {
                    plan += "_";
                    execute += "_";
                    resultSetSize += "_";
                    break;
                } else {
                    benchmarkQueryInBaselineApproach(gm, queryId, shuffledQuery.toString());
                }
            }
            // Execute Query as it is
            if (benchmarkQueryGivenOrder) {
                if (timeOutFlagForCurrentQuery) {
                    plan += "_";
                    execute += "_";
                    resultSetSize += "_";
                    break;
                } else {
                    benchmarkQueryGivenOrder(gm, queryId, q.toString());
                }
            }
            
            if (benchmarkQueryOtherPurpose) {
                if (timeOutFlagForCurrentQuery) {
                    plan += "_";
                    execute += "_";
                    resultSetSize += "_";
                    break;
                } else {
                    OtherPurposes(gm, queryId, q.toString());
                }
            }
            
            if (!(i == counter - 1)) {
                this.plan += ",";
                this.execute += ",";
                this.resultSetSize += ",";
            }
        }
        tuple += plan + ",-," + execute + ",-," + resultSetSize + "\n";
        writeToFile(tuple, "Benchmark.csv", false);
        if (!ds.isInTransaction())
            ds.close();
        
      /*  if (!plan.contains("_")) {
            int[] planningTimeArray = Stream.of(plan.split(",")).mapToInt(Integer::parseInt).toArray();
            int[] executionTimeArray = Stream.of(execute.split(",")).mapToInt(Integer::parseInt).toArray();
            int[] runTimeArray = new int[planningTimeArray.length];
            for (int i = 0; i < planningTimeArray.length; i++) {
                runTimeArray[i] = planningTimeArray[i] + executionTimeArray[i];
            }
            System.out.println();
            System.out.println("Avg RunTime: " + Arrays.stream(runTimeArray).average().toString());
        }*/
        System.out.println();
    }
    
    private void benchmarkQueryInJena(Model gm, String queryId, String query) {
        
        //shuffle input query Triple patterns
        query = shuffleQueryTPs(query);
        
        //Planning
        long start = System.currentTimeMillis();
        QueryExecution qExec = QueryExecutionFactory.create(QueryFactory.create(query), gm);
        setLoggingLevel(qExec);
        ResultSet rs = qExec.execSelect();
        long end = System.currentTimeMillis() - start;
        
        plan += end;
        
        //Execution
        start = System.currentTimeMillis();
        int rsSize = iterateOverResultsWithTimeOut(rs);
        end = System.currentTimeMillis() - start;
        
        execute += end;
        
        //Execution Size
        resultSetSize += rsSize;
        qExec.close();
    }
    
    private void benchmarkQueryInShapeApproach(Model gm, String queryId, String query) {
        
        //shuffle input query Triple patterns
        query = shuffleQueryTPs(query);
        
        if (Boolean.parseBoolean(ConfigManager.getProperty("queryPlanAnalysisOnly"))) {
            TriplesEvaluatorWithPlan queryEvaluator = new TriplesEvaluatorWithPlan(query);
            Op op = queryEvaluator.designQueryPlan(queryId);
        } else {
            //Planning
            long start = System.currentTimeMillis();
            PlanMaker queryEvaluator = new PlanMaker(query);
            Op op = queryEvaluator.getJoinOrderingViaShapeApproachV2(queryId);
            long end = System.currentTimeMillis() - start;
            plan += end;
            
            //Execution
            start = System.currentTimeMillis();
            if (!ds.isInTransaction()) {
                ds.begin(ReadWrite.READ);
            }
            QueryIterator queryIterator = Algebra.exec(op, gm.getGraph());
            ResultSet rs = new ResultSetStream(queryEvaluator.getProjectedVariables(), gm, queryIterator);
            int rsSize = iterateOverResultsWithTimeOut(rs);
            end = System.currentTimeMillis() - start;
            execute += end;
            //Execution Size
            resultSetSize += rsSize;
            queryIterator.close();
        }
    }
    
    private void benchmarkQueryInBaselineApproach(Model gm, String queryId, String query) {
        //shuffle input query Triple patterns
        query = shuffleQueryTPs(query);
        
        //Planning
        long start = System.currentTimeMillis();
        PlanMaker queryEvaluator = new PlanMaker(query);
        Op op = queryEvaluator.getJoinOrderingViaBaselineApproach(queryId);
        long end = System.currentTimeMillis() - start;
        plan += end;
        
        //Execution
        start = System.currentTimeMillis();
        if (!ds.isInTransaction()) {
            ds.begin(ReadWrite.READ);
        }
//        QueryIterator queryIterator = Algebra.exec(op, gm.getGraph());
//        ResultSet rs = new ResultSetStream(queryEvaluator.getProjectedVariables(), gm, queryIterator);
//        int rsSize = iterateOverResultsWithTimeOut(rs);
        end = System.currentTimeMillis() - start;
        execute += end;
        //Execution Size
//        resultSetSize += rsSize;
//        queryIterator.close();
    }
    
    private void OtherPurposes(Model gm, String queryId, String query) {
        
        //1.Estimate Cost
        PlanMaker planMaker = new PlanMaker(query);
        planMaker.estimateCost(queryId);
        
        //2.generate queries in random order
        /*for (int i = 0; i < 9; i++) {
            query = shuffleQueryTPs(query);
            Op op = QueryUtils.constructQueryAndPreserveTheGivenOrder(QueryFactory.create(query), queryId);
            Query q = OpAsQuery.asQuery(op);
            RdfUtils.writeToFile(q.toString(),
                    "/Users/user/Documents/GitHub/RDFShapes/src/main/resources/lubm_resources/lubmQueries/variousQueries/s5/" + queryId.split(".sparql")[0] + "_" + i + ".sparql");
        }*/
        
    }
    
    private void benchmarkQueryGivenOrder(Model gm, String queryId, String query) {
        logger.info("Executing Query In the given order of TPs");
        //Planning
        long start = System.currentTimeMillis();
        Query q = QueryFactory.create(query);
        Op op = QueryUtils.constructQueryAndPreserveTheGivenOrder(q, queryId);
        
        System.out.println(op);
        long end = System.currentTimeMillis() - start;
        plan += end;
        
        //Execution
        start = System.currentTimeMillis();
        if (!ds.isInTransaction()) {
            ds.begin(ReadWrite.READ);
        }
        QueryIterator queryIterator = Algebra.exec(op, gm.getGraph());
        ResultSet rs = new ResultSetStream(Lists.transform(q.getProjectVars(), Functions.toStringFunction()), gm, queryIterator);
        int rsSize = iterateOverResultsWithTimeOut(rs);
        end = System.currentTimeMillis() - start;
        execute += end;
        //Execution Size
        resultSetSize += rsSize;
        queryIterator.close();
    }
    
    public int iterateOverResults(ResultSet resultSet) {
        int iterationCounter = 0;
        
        while (resultSet.hasNext()) {
            Binding b = resultSet.nextBinding();
            //System.out.println(b.toString());
            iterationCounter++;
        }
        return iterationCounter;
    }
    
    public int iterateOverResultsWithTimeOut(ResultSet resultSet) {
        long limit = System.currentTimeMillis() + queryTimeOutInMS;
        int itc = 0;
        try {
            while (resultSet.hasNext()) {
                Binding b = resultSet.nextBinding();
                //System.out.println(b.toString());
                if (System.currentTimeMillis() > limit) {
                    //timeOutFlag = true;
                    this.timeOutFlagForCurrentQuery = true;
                    break;
                }
                itc++;
            }
        } catch (StorageException storageException) {
            System.out.println(storageException.getMessage());
        }
        
        return itc;
    }
    
    public static void writeToFile(String str, String fileNameAndPath, boolean append) {
        try {
            String address = ConfigManager.getProperty("queryResultOutputDirectory");
            FileWriter fileWriter = new FileWriter(new File(address, fileNameAndPath), append);
            PrintWriter printWriter = new PrintWriter(fileWriter);
            printWriter.print(str);
            printWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public void setLoggingLevel(QueryExecution qExec) {
        if (ConfigManager.getProperty("showJenaExecutionPlan").equals("true")) {
            qExec.getContext().set(ARQ.symLogExec, Explain.InfoLevel.ALL);
        }
    }
    
    private void executeTheGivenQueryAsItIs(String q) {
        if (Objects.equals(ConfigManager.getProperty("executeQueryAsItIs"), "true")) {
            logger.info("Executing Query as it is....");
            long t1 = System.currentTimeMillis();
            Query query = QueryFactory.create(q);
            Op op = Algebra.compile(query);
            
            Dataset ds = RdfUtils.getTDBDataset();
            Model gm = ds.getNamedModel(Main.getRdfModelIRI());
            if (!ds.isInTransaction())
                ds.begin(ReadWrite.READ);
            
            QueryIterator queryIterator = Algebra.exec(op, gm.getGraph());
            ResultSet resultSet = new ResultSetStream(Lists.transform(query.getProjectVars(), Functions.toStringFunction()), gm, queryIterator);
            logger.info("Result set size: " + iterateOverResults(resultSet));
            System.out.println(System.currentTimeMillis() - t1);
            queryIterator.close();
            ds.end();
            ds.close();
        }
    }
    
    private String shuffleQueryTPs(String query) {
        TriplesShuffler triplesShuffler = new TriplesShuffler(query);
        return triplesShuffler.shuffleSingleQueryTPs();
    }
}
