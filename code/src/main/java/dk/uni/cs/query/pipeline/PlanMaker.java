package dk.uni.cs.query.pipeline;

import dk.uni.cs.query.pipeline.tba.*;
import dk.uni.cs.query.pipeline.sba.SBA;
import dk.uni.cs.shapes.ShapesDetector;
import dk.uni.cs.utils.RdfUtils;
import dk.uni.cs.utils.Tuple3;
import org.apache.jena.ext.com.google.common.base.Functions;
import org.apache.jena.ext.com.google.common.collect.Lists;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.core.Var;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;


public class PlanMaker {
    private final List<Var> projectedVariables;
    HashMap<Triple, List<Tuple3<String, String, String>>> triplesToShapesMapping;
    Query query;
    ArrayList<HashSet<Triple>> bgps;
    Op algebraOp = null;
    
    public PlanMaker(String posedQuery) {
        this.query = QueryFactory.create(posedQuery);
        this.projectedVariables = this.query.getProjectVars();
        bgps = QueryUtils.getBGPs(query);
    }
    
    public Op getJoinOrderingViaShapeApproachV1(String queryId) {
        System.out.println("SHAPE APPROACH - V1");
        HashSet<Triple> triples = bgps.get(0);
        
        //Get BGPs -> Candidate Shapes -> Statistics
        ShapesDetector sd = new ShapesDetector(triples, true);
        
        //SBA
        SBA sba = new SBA(query, triples, sd.getTriplesWithStats(), sd.getIsTriplePatternTypeDefined());
        Op algebraSBA = sba.opSBA();
        
        //TBA
        TBA tba = new TBA(query, triples, sd.getTriplesWithStats(), sd.getIsTriplePatternTypeDefined());
        tba.setQueryId(queryId);
        //tba.setStarsInfo(sba);
        Op algebraTBA = tba.opTBA_V1();
        
        //Logs
        System.out.println("SBA: " + sba.getGlobalPlanCost() + " -> Algebra: " + algebraSBA);
        System.out.println("TBA: " + tba.getGlobalPlanCost() + " -> Algebra: " + algebraTBA);
        
        //Decide which join ordering to choose, SBA or TBA?
        if (sba.getGlobalPlanCost() < tba.getGlobalPlanCost()) algebraOp = algebraSBA;
        else algebraOp = algebraTBA;
        
        // An exception
        if (tba.getGlobalPlanCost() == 0) algebraOp = algebraSBA;
        
        System.out.println(algebraOp);
        return algebraOp;
    }
    
    
    public Op getJoinOrderingViaShapeApproachV2(String queryId) {
        System.out.println("SHAPE APPROACH - V2");
        HashSet<Triple> triples = bgps.get(0);
        
        //Get BGPs -> Candidate Shapes -> Statistics
        ShapesDetector sd = new ShapesDetector(triples, true);
        
        //TBA
        TBA tba = new TBA(query, triples, sd.getTriplesWithStats(), sd.getIsTriplePatternTypeDefined());
        tba.setStarsInfo(new SBA(query, triples, sd.getTriplesWithStats(), sd.getIsTriplePatternTypeDefined()));
        tba.setQueryId(queryId);
        algebraOp = tba.opTBA_V2();
        
        //Logs
        System.out.println("TBA via Shape Approach: " + tba.getGlobalPlanCost() + " -> Algebra: " + algebraOp);
        System.out.println(algebraOp);
        saveOptimizedQueryInFile(queryId);
        return algebraOp;
    }
    
    public Op getJoinOrderingViaBaselineApproach(String queryId) {
        System.out.println("BASELINE APPROACH");
        HashSet<Triple> triples = bgps.get(0);
        
        //Get BGPs -> Candidate Shapes -> Statistics
        ShapesDetector sd = new ShapesDetector(triples, false);
        
        //TBA
        TBA tba = new TBA(query, triples, sd.getTriplesWithStats(), sd.getIsTriplePatternTypeDefined());
        tba.setQueryId(queryId);
        algebraOp = tba.opTBA_GlobalStats();
        
        //Logs
        System.out.println("TBA for Baseline Execution: " + tba.getGlobalPlanCost() + " -> Algebra: " + algebraOp);
        saveOptimizedQueryInFile(queryId);
        System.out.println(algebraOp);
        return algebraOp;
    }
    
    public void estimateCost(String queryId) {
        System.out.println("Estimating Cost");
        HashSet<Triple> triples = bgps.get(0);
    
        //Get BGPs -> Candidate Shapes -> Statistics
        ShapesDetector sd = new ShapesDetector(triples, true);
    
        //TBA
        TBA tba = new TBA(query, triples, sd.getTriplesWithStats(), sd.getIsTriplePatternTypeDefined());
        tba.setStarsInfo(new SBA(query, triples, sd.getTriplesWithStats(), sd.getIsTriplePatternTypeDefined()));
        tba.setQueryId(queryId);
        tba.costComputationWithFixedOrdering(QueryUtils.getTriplePatterns(query));
    }
    
    private void saveOptimizedQueryInFile(String queryId) {
        Query optimizedQuery = OpAsQuery.asQuery(algebraOp);
        System.out.println("******** THE QUERY ********");
        System.out.println(optimizedQuery);
        //RdfUtils.writeToFile(optimizedQuery.toString(), "/Users/user/Documents/GitHub/RDFShapes/src/main/resources/watDiv_resources/watDivQueries/Large/ssQueries/" + queryId);
        //RdfUtils.writeToFile(optimizedQuery.toString(), "/Users/user/Documents/GitHub/RDFShapes/src/main/resources/yago_resources/yagoQueries/gsQueries/" + queryId);
        System.out.println("****************");
    }
    
    //Getter Functions
    List<String> getProjectedVariables() {
        return Lists.transform(projectedVariables, Functions.toStringFunction());
    }
    
    public List<Var> getVarProjectedVariables() {
        return projectedVariables;
    }
    
    
}